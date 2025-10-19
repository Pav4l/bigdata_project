# Автоматически сгенерированный DAG для файла example_py.py
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import importlib.util
import sys
from pathlib import Path

DEFAULT_CONN_IDS = {'DB_CONN_ID': 'postgres_default'}


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
    dag_id="gen_example_py",
    schedule_interval='0 2 * * *',
    start_date=datetime(2025, 10, 19),
    catchup=False,
    tags=["generated"]
) as dag:
    extract = PythonOperator(
        task_id="extract",
        python_callable=_call_func,
        op_args=[r"sources\example_py.py", "extract"],
    )
    transform = PythonOperator(
        task_id="transform",
        python_callable=_call_func,
        op_args=[r"sources\example_py.py", "transform"],
    )
    load = PythonOperator(
        task_id="load",
        python_callable=_call_func,
        op_args=[r"sources\example_py.py", "load"],
    )

    extract >> transform >> load
