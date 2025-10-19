# Автоматически сгенерированный SQL DAG для файла example_sql.sql
from datetime import datetime
from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator

DEFAULT_CONN_ID = "pg_extra       -- опциональный connection id, если не указан, используется postgres_default"

with DAG(
    dag_id="gen_example_sql",
    schedule_interval='30 3 * * *    -- cron через комментарий (строка после "SCHEDULE:")',
    start_date=datetime(2025, 10, 19),
    catchup=False,
    tags=["generated", "sql"]
) as dag:
    sql_1 = PostgresOperator(
        task_id="sql_1",
        postgres_conn_id=DEFAULT_CONN_ID,
        sql="""-- SCHEDULE: 30 3 * * *    -- cron через комментарий (строка после "SCHEDULE:")
    -- CONN_ID: pg_extra       -- опциональный connection id, если не указан, используется postgres_default

    CREATE TABLE IF NOT EXISTS test_table (
        id serial PRIMARY KEY,
        name text
    );""",
    )
    sql_2 = PostgresOperator(
        task_id="sql_2",
        postgres_conn_id=DEFAULT_CONN_ID,
        sql="""INSERT INTO test_table (name) VALUES ('alpha');""",
    )
    sql_3 = PostgresOperator(
        task_id="sql_3",
        postgres_conn_id=DEFAULT_CONN_ID,
        sql="""ALTER TABLE test_table ADD COLUMN created_at timestamp DEFAULT now();""",
    )

    sql_1 >> sql_2 >> sql_3
