import asyncio
import os
import time
import random
from aiobotocore.session import get_session
from contextlib import asynccontextmanager
import requests
from bs4 import BeautifulSoup
import pandas as pd
import aiofiles
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

load_dotenv()


def fetch_page(url):
    """Загружает страницу с обработкой ошибок и повторными попытками."""
    for attempt in range(CONFIG["max_retries"]):
        try:
            response = requests.get(url, headers=CONFIG["headers"], timeout=10)
            if response.status_code == 200:
                return response.text
            else:
                print(f"Ошибка {response.status_code}, повтор {attempt+1}")
        except requests.exceptions.RequestException as e:
            print(f"Ошибка соединения: {e}, попытка {attempt+1}")
        time.sleep(2)
    return None


def parse_page(html):
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


def crawl_site():
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


def save_to_csv(data, filename="books.csv"):
    """Сохраняет данные в CSV и делает простую агрегацию."""
    df = pd.DataFrame(data)
    df.to_csv(filename, index=False, encoding="utf-8")
    print(f"Данные сохранены в {filename}")

    # Проверка целостности: сколько строк и уникальных названий
    print("Всего записей:", len(df))
    print("Уникальных книг:", df['title'].nunique())

    # Пример агрегации: топ-5 самых частых цен
    print(df['price'].value_counts().head())


class AsyncObjectStorage:
    def __init__(self, *, key_id: str, secret: str,
                 endpoint: str, container: str):
        self._auth = {
            "aws_access_key_id": key_id,
            "aws_secret_access_key": secret,
            "endpoint_url": endpoint,
        }
        self._bucket = container
        self._session = get_session()

    @asynccontextmanager
    async def _connect(self):
        async with self._session.create_client("s3",
                                               **self._auth) as connection:
            yield connection

    async def send_file(self, local_path: str):
        """Загружает файл в Selectel Object Storage"""
        filename = os.path.basename(local_path)
        async with self._connect() as remote:
            async with aiofiles.open(local_path, "rb") as f:
                data = await f.read()
                await remote.put_object(Bucket=self._bucket,
                                        Key=filename, Body=data)


async def upload_to_selectel(local_file: str):
    key = os.getenv("key_id")
    secret = os.getenv("secret_key")

    if not key or not secret:
        raise RuntimeError("❌ Не найдены ключи доступа. Проверьте .env (key_id, secret_key)")

    storage = AsyncObjectStorage(
        key_id=key,
        secret=secret,
        endpoint="https://s3.ru-7.storage.selcloud.ru",
        container="data-engineer-practice-pavel3"  # контейнер Selectel
    )

    await storage.send_file(local_file)
    print(f"✅ Файл {local_file} успешно загружен в Selectel.")


if __name__ == "__main__":
    books_data = crawl_site()
    save_to_csv(books_data)
    asyncio.run(upload_to_selectel("books.csv"))
