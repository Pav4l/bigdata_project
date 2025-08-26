from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime


def user_function():
    print("Doing something inside Airflow!")
    for i in range(3):
        print(f'Step {i}')


with DAG(
    dag_id="generated_py_do_something",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["py-auto"]
) as dag:

    run_python = PythonOperator(
        task_id="run_python_function",
        python_callable=user_function
    )
