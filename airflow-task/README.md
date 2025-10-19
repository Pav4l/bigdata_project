# Генератор DAG для Apache Airflow

Этот проект автоматически генерирует DAG’и для Apache Airflow на основе исходных файлов Python и SQL. Каждый файл в папке sources/ превращается в отдельный DAG в папке dags/generated/.

## Запуск Airflow через Docker

```bash
docker-compose up -d
```

## Генерация DAG

```bash
python generate_dags.py
```