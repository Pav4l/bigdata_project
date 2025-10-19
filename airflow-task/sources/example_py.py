# -*- coding: utf-8 -*-

# Обязательное поле: CRON / SCHEDULE (строка cron или presets like '@daily')
SCHEDULE = "0 2 * * *"  # каждый день в 02:00

# Рекомендуется: connection ids для операторов (только id, не креды).
# При отсутствии используем дефолтные значения в генераторе / DAG.
DB_CONN_ID = "postgres_default"


def extract():
    print("extract: получаем данные")


def transform():
    print("transform: чистим / трансформируем")


def load():
    print("load: сохраняем")
