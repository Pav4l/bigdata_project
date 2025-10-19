"""
Генератор DAG'ов для Apache Airflow.

Функционал:
 - Просматривает папку `sources/`
 - Для каждого файла создаёт отдельный DAG в `dags/generated/`
 - Поддерживаются форматы: `.py` и `.sql`
 - Для Python-файлов каждая функция (def) превращается в отдельный таск (PythonOperator)
 - Для SQL-файлов каждая команда DDL/DML превращается в отдельный таск (PostgresOperator)
 - Дата старта DAG'а — день генерации
 - Расписание (SCHEDULE) и подключение (CONN_ID) берутся из исходников
 - В DAG не записываются параметры подключения (используются connection_id из Airflow)

"""

import os
import ast
import re
from datetime import date
from pathlib import Path
import textwrap

# Папка с исходными файлами (Python и SQL)
SOURCES_DIR = Path("sources")

# Папка, куда будут сохраняться сгенерированные DAG-файлы
OUT_DIR = Path("dags/generated")
OUT_DIR.mkdir(parents=True, exist_ok=True)

# Ключевые слова SQL, по которым можно делить файл на отдельные задания
DDL_KEYWORDS = ["CREATE", "ALTER", "DROP", "INSERT", "UPDATE", "DELETE", "TRUNCATE", "CREATE OR REPLACE"]


def slugify(s: str) -> str:
    """Упрощение строки: удаление недопустимых символов и привеление к нижнему регистру"""
    return re.sub(r"[^0-9a-zA-Z_]+", "_", s).strip("_").lower()


def parse_python_file(path: Path):
    """
    Разбор Python-скрипт:
      - возврат расписания (schedule)
      - возврат найденных conn_id (если есть)
      - возврат списка функций (def)
    """
    src = path.read_text(encoding="utf-8")
    tree = ast.parse(src, filename=str(path))
    funcs = [n.name for n in tree.body if isinstance(n, ast.FunctionDef)]

    schedule = None
    conn_ids = {}

    # Ищем переменные SCHEDULE, CRON и *_CONN_ID
    for node in tree.body:
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name):
                    name = target.id
                    if name.upper() in ("SCHEDULE", "CRON"):
                        # Пробуем считать значение переменной
                        if isinstance(node.value, ast.Constant):
                            schedule = node.value.value
                    if name.upper().endswith("CONN_ID") or name.upper().endswith("CONNID"):
                        if isinstance(node.value, ast.Constant):
                            conn_ids[name] = node.value.value
    return schedule, conn_ids, funcs


def parse_sql_file(path: Path):
    """
    Разбор SQL-файл:
      - извлечение параметров из заголовка (-- SCHEDULE:, -- CONN_ID:)
      - разделение SQL на отдельные выражения
    """
    txt = path.read_text(encoding="utf-8")

    schedule = None
    conn_id = None
    header_lines = []

    # Читаем комментарии в начале файла (для SCHEDULE и CONN_ID)
    for ln in txt.splitlines():
        ln_strip = ln.strip()
        if ln_strip.startswith("--"):
            header_lines.append(ln_strip[2:].strip())
        elif ln_strip == "":
            continue
        else:
            break

    for hl in header_lines:
        m = re.match(r"(?i)SCHEDULE\s*:\s*(.+)", hl)
        if m:
            schedule = m.group(1).strip()
        m2 = re.match(r"(?i)CONN_ID\s*:\s*(.+)", hl)
        if m2:
            conn_id = m2.group(1).strip()

    # Делим SQL по точке с запятой
    statements = []
    buffer = []
    for line in txt.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("--"):
            buffer.append(line)
            continue
        buffer.append(line)
        if ";" in line:
            stmt = "\n".join(buffer).strip()
            if stmt:
                statements.append(stmt)
            buffer = []
    if buffer:
        stmt = "\n".join(buffer).strip()
        if stmt:
            statements.append(stmt)

    # Возвращаем список SQL-команд
    return schedule, conn_id, statements


def generate_dag_for_python(src_path: Path, schedule, conn_ids, funcs, dag_start_date):
    """
    Генерирация DAG-файла для Python-скрипта
    """
    name = src_path.stem
    dag_id = f"gen_{slugify(name)}"
    py_rel = os.path.relpath(src_path.resolve(), Path.cwd())

    content = f'''\
# Автоматически сгенерированный DAG для файла {src_path.name}
from datetime import datetime, date
from airflow import DAG
from airflow.operators.python import PythonOperator
import importlib.util
import sys
from pathlib import Path

DEFAULT_CONN_IDS = {conn_ids or {{}}}

def _call_func(module_path, func_name):
    """
    Импорт модуля по пути и вызов указанной функции без аргументов.
    """
    p = Path(module_path)
    if str(p.parent) not in sys.path:
        sys.path.insert(0, str(p.parent))
    spec = importlib.util.spec_from_file_location(p.stem, str(p))
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    fn = getattr(mod, func_name)
    return fn()

with DAG(
    dag_id="{dag_id}",
    schedule_interval={repr(schedule)},
    start_date=datetime({dag_start_date.year}, {dag_start_date.month}, {dag_start_date.day}),
    catchup=False,
    tags=["generated"]
) as dag:
'''
    if not funcs:
        content += "\n    # В модуле не найдено ни одной функции (def). Добавьте хотя бы одну.\n"
    else:
        # Создаём таски по функциям
        for fn in funcs:
            task_id = slugify(fn)
            content += textwrap.indent(f'''{task_id} = PythonOperator(
    task_id="{task_id}",
    python_callable=_call_func,
    op_args=[r"{py_rel}", "{fn}"],
)\n''', '    ')
        # Последовательная цепочка задач (можно поменять на параллельную)
        if len(funcs) > 1:
            seq = " >> ".join([slugify(f) for f in funcs])
            content += "\n    " + seq + "\n"
    return content


def generate_dag_for_sql(src_path: Path, schedule, conn_id, statements, dag_start_date):
    """
    Генерирация DAG-файла для SQL-скрипта
    """
    name = src_path.stem
    dag_id = f"gen_{slugify(name)}"

    content = f'''\
# Автоматически сгенерированный SQL DAG для файла {src_path.name}
from datetime import datetime, date
from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator

DEFAULT_CONN_ID = "{conn_id or 'postgres_default'}"

with DAG(
    dag_id="{dag_id}",
    schedule_interval={repr(schedule)},
    start_date=datetime({dag_start_date.year}, {dag_start_date.month}, {dag_start_date.day}),
    catchup=False,
    tags=["generated", "sql"]
) as dag:
'''
    if not statements:
        content += "\n    # Пустой SQL-файл — задач не создано\n"
        return content

    # Создаём таски для каждого SQL-блока
    for idx, stmt in enumerate(statements, start=1):
        snippet = re.sub(r"\s+", " ", stmt).strip()
        task_id = f"sql_{idx}"
        safe_sql = stmt.replace('"""', r'\"\"\"')
        content += textwrap.indent(f'''{task_id} = PostgresOperator(
    task_id="{task_id}",
    postgres_conn_id=DEFAULT_CONN_ID,
    sql=\"\"\"{safe_sql}\"\"\",
)\n''', '    ')

    # Цепляем задачи последовательно
    if len(statements) > 1:
        seq = " >> ".join([f"sql_{i}" for i in range(1, len(statements)+1)])
        content += "\n    " + seq + "\n"

    return content


def main():
    """Главная функция генератора DAG'ов"""
    dag_start_date = date.today()

    for path in sorted(SOURCES_DIR.glob("*")):
        if path.suffix.lower() == ".py":
            schedule, conn_ids, funcs = parse_python_file(path)
            if schedule is None:
                print(f"[ПРЕДУПРЕЖДЕНИЕ] {path.name}: не найдено поле SCHEDULE/CRON — используется '@daily' по умолчанию.")
                schedule = "@daily"
            content = generate_dag_for_python(path, schedule, conn_ids, funcs, dag_start_date)

        elif path.suffix.lower() == ".sql":
            schedule, conn_id, statements = parse_sql_file(path)
            if schedule is None:
                print(f"[ПРЕДУПРЕЖДЕНИЕ] {path.name}: не найдено поле SCHEDULE — используется '@daily' по умолчанию.")
                schedule = "@daily"
            content = generate_dag_for_sql(path, schedule, conn_id, statements, dag_start_date)

        else:
            print(f"[ПРОПУЩЕНО] {path.name}: неподдерживаемое расширение файла")
            continue

        out_file = OUT_DIR / f"dag_{slugify(path.stem)}.py"
        out_file.write_text(content, encoding="utf-8")
        print(f"[OK] Сгенерирован DAG: {out_file}")


if __name__ == "__main__":
    main()
