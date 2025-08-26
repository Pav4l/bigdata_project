from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime

default_args = {
    'start_date': datetime(2024, 1, 1),
}

with DAG(
    dag_id='pg_create_insert_select_nolog',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    description='Создание таблицы, вставка и чтение данных без вывода в лог',
) as dag:

    create_table = PostgresOperator(
        task_id='create_table',
        postgres_conn_id='pg_extra_conn',
        sql="""
        CREATE TABLE IF NOT EXISTS sample_data (
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL,
            value INTEGER
        );
        """
    )

    insert_data = PostgresOperator(
        task_id='insert_data',
        postgres_conn_id='pg_extra_conn',
        sql="""
        INSERT INTO sample_data (name, value)
        VALUES
            ('alpha', 10),
            ('beta', 20),
            ('gamma', 30);
        """
    )

    select_data = PostgresOperator(
        task_id='select_data',
        postgres_conn_id='pg_extra_conn',
        sql="SELECT * FROM sample_data;"
    )

    create_table >> insert_data >> select_data
