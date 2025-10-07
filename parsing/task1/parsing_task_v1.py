import os
import time
import random
from pathlib import Path

import requests
from bs4 import BeautifulSoup
import pandas as pd
import boto3
from botocore.exceptions import BotoCoreError, ClientError
from dotenv import load_dotenv

# =====================
# Конфигурация парсера
# =====================
CONFIG = {
    "base_url": "http://books.toscrape.com/catalogue/page-{}.html",
    "headers": {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"},
    "delay_range": (1, 3),       # задержка между запросами (в секундах)
    "max_retries": 3             # число повторных попыток при ошибке
}

load_dotenv()  # загрузим .env (key_id, secret_key и т.п.)

OUTPUT_DIR = Path("output")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def fetch_page(url: str) -> str | None:
    """Загружает страницу с обработкой ошибок и повторными попытками."""
    for attempt in range(CONFIG["max_retries"]):
        try:
            resp = requests.get(url, headers=CONFIG["headers"], timeout=10)
            if resp.status_code == 200:
                return resp.text
            else:
                print(f"Ошибка {resp.status_code}, повтор {attempt+1}")
        except requests.exceptions.RequestException as e:
            print(f"Ошибка соединения: {e}, попытка {attempt+1}")
        time.sleep(2)
    return None


def parse_page(html: str) -> list[dict]:
    """Парсит HTML и возвращает список книг со страницы."""
    soup = BeautifulSoup(html, "html.parser")
    books = []

    for book in soup.select(".product_pod"):
        title = book.h3.a["title"]
        price = book.select_one(".price_color").text.strip()
        availability = book.select_one(".availability").text.strip()

        books.append({
            "title": title,
            "price": price,
            "availability": availability
        })
    return books


def crawl_site() -> list[dict]:
    """Обходит все страницы каталога и собирает данные."""
    all_books = []
    page_num = 1

    while True:
        url = CONFIG["base_url"].format(page_num)
        html = fetch_page(url)
        if not html:
            break

        books = parse_page(html)
        if not books:  # если книг больше нет → конец
            break

        all_books.extend(books)
        print(f"Собрано книг со страницы {page_num}: {len(books)}")

        page_num += 1
        time.sleep(random.uniform(*CONFIG["delay_range"]))

    return all_books


def save_to_csv(data: list[dict], filename: str = "books.csv") -> Path:
    """Сохраняет данные в CSV и делает простую агрегацию. Возвращает путь к файлу."""
    df = pd.DataFrame(data)
    output_path = OUTPUT_DIR / filename
    df.to_csv(output_path, index=False, encoding="utf-8")
    print(f"Данные сохранены в {output_path}")

    # Проверка целостности: сколько строк и уникальных названий
    print("Всего записей:", len(df))
    print("Уникальных книг:", df['title'].nunique())

    # Пример агрегации: топ-5 самых частых цен
    print(df['price'].value_counts().head())

    return output_path


# ====== Sync storage (boto3) ======
class SyncObjectStorage:
    def __init__(self, *, key_id: str, secret: str, endpoint: str, container: str):
        # проверка аргументов
        if not key_id or not secret:
            raise RuntimeError("AWS credentials are required (key_id, secret_key).")

        # создаём клиент boto3 с указанием endpoint (Selectel S3 совместимый)
        self._client = boto3.client(
            's3',
            aws_access_key_id=key_id,
            aws_secret_access_key=secret,
            endpoint_url=endpoint
        )
        self._bucket = container

    def upload_file(self, local_path: str, remote_name: str | None = None):
        """Загружает файл в S3-совместимое хранилище."""
        if remote_name is None:
            remote_name = Path(local_path).name

        try:
            # Используем upload_file (эффективно для больших файлов)
            self._client.upload_file(Filename=local_path, Bucket=self._bucket, Key=remote_name)
            print(f"✅ Uploaded {local_path} -> s3://{self._bucket}/{remote_name}")
        except (BotoCoreError, ClientError) as e:
            print(f"Ошибка при upload: {e}")
            raise


def upload_to_selectel_sync(local_file: str):
    key = os.getenv("key_id")
    secret = os.getenv("secret_key")
    endpoint = "https://s3.ru-7.storage.selcloud.ru"
    container = "data-engineer-practice-pavel3"

    if not key or not secret:
        raise RuntimeError("❌ Не найдены ключи доступа. Проверьте .env (key_id, secret_key)")

    storage = SyncObjectStorage(key_id=key, secret=secret, endpoint=endpoint, container=container)
    storage.upload_file(local_file)


if __name__ == "__main__":
    books_data = crawl_site()
    csv_path = save_to_csv(books_data, filename="books.csv")
    # синхронная загрузка
    upload_to_selectel_sync(str(csv_path))
