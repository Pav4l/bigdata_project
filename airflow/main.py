import os
from jinja2 import Template

BASE_DIR = os.path.dirname(__file__)
SCRIPTS_DIR = os.path.join(BASE_DIR, "scripts")
TEMPLATES_DIR = os.path.join(BASE_DIR, "templates")
OUTPUT_DIR = os.path.join(BASE_DIR, "dags")

TEMPLATE_MAP = {
    ".sql": "sql_template.j2",
    ".py": "python_template.j2"
}


def load_template(template_path):
    with open(template_path, encoding="utf-8") as f:
        return Template(f.read())


def generate_dag_file(script_name, code, template_name):
    file_base = os.path.splitext(script_name)[0]
    ext = os.path.splitext(script_name)[1]
    dag_id = f"generated_{ext[1:]}_{file_base}"

    template_path = os.path.join(TEMPLATES_DIR, template_name)
    template = load_template(template_path)

    rendered = template.render(
        dag_id=dag_id,
        sql_code=code if ext == ".sql" else "",
        python_code=code if ext == ".py" else ""
    )

    output_path = os.path.join(OUTPUT_DIR, f"{dag_id}.py")
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(rendered)
    print(f"✅ Created DAG: {output_path}")


def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    for fname in os.listdir(SCRIPTS_DIR):
        ext = os.path.splitext(fname)[1]
        if ext not in TEMPLATE_MAP:
            continue

        with open(os.path.join(SCRIPTS_DIR, fname), encoding="utf-8") as f:
            code = f.read()

        generate_dag_file(fname, code, TEMPLATE_MAP[ext])


if __name__ == "__main__":
    main()
