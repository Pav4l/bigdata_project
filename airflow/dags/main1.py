from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime


# === Подключение к дефолтной базе postgres ===
def get_hook():
    return PostgresHook(
        postgres_conn_id='pg_extra_conn'
    )


# === Шаг 1: Создание таблицы ===
def create_table():
    hook = get_hook()
    with hook.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS sample_data (
                    id SERIAL PRIMARY KEY,
                    name TEXT NOT NULL,
                    value INTEGER
                )
            """)
            conn.commit()


# === Шаг 2: Вставка данных ===
def insert_data():
    hook = get_hook()
    with hook.get_conn() as conn:
        with conn.cursor() as cur:
            cur.executemany("""
                INSERT INTO sample_data (name, value) VALUES (%s, %s)
            """, [
                ('alpha', 10),
                ('beta', 20),
                ('gamma', 30),
            ])
            conn.commit()


# === Шаг 3: Вывод данных в лог ===
def log_data():
    hook = get_hook()
    with hook.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM sample_data")
            rows = cur.fetchall()
            print("Содержимое таблицы sample_data:")
            for row in rows:
                print(row)


# === DAG ===
with DAG(
    dag_id='pg_extra_create_and_log',
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    description='Создание таблицы, вставка и логирование данных в pg_extra (база postgres)',
) as dag:

    t1 = PythonOperator(
        task_id='create_table',
        python_callable=create_table
    )

    t2 = PythonOperator(
        task_id='insert_data',
        python_callable=insert_data
    )

    t3 = PythonOperator(
        task_id='log_data',
        python_callable=log_data
    )

    t1 >> t2 >> t3
