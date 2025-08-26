from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime


def run_sql():
    sql = """
DELETE FROM logs WHERE created_at < NOW() - INTERVAL '30 days';
    """
    hook = PostgresHook(postgres_conn_id="postgres_default")
    hook.run(sql)


with DAG(
    dag_id="generated_sql_cleanup_data",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["sql-auto"]
) as dag:

    execute_sql = PythonOperator(
        task_id="execute_sql",
        python_callable=run_sql
    )
