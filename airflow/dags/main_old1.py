from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

print("Теперь я вне власти питона")

# === Функция, которую вызовет оператор ===
def my_python_task():
    print("Привет из Python!")

# === Определение DAG ===
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 0,
}

dag = DAG(
    dag_id='example_python_operator_dag',
    default_args=default_args,
    schedule_interval=None,  # Запуск только вручную
    catchup=False,
    description='DAG с использованием PythonOperator',
)

# === Оператор ===
python_task = PythonOperator(
    task_id='my_task',
    python_callable=my_python_task,
    dag=dag,
)

# === Последовательность задач ===
python_task
