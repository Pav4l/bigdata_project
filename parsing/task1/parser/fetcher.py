import time
import random
import requests
from .parser import parse_page
from utils.config import CONFIG


def fetch_page(url: str) -> str | None:
    """Загружает HTML страницу с повторными попытками."""
    for attempt in range(CONFIG["max_retries"]):
        try:
            resp = requests.get(url, headers=CONFIG["headers"], timeout=10)
            if resp.status_code == 200:
                return resp.text
            print(f"Ошибка {resp.status_code}, повтор {attempt+1}")
        except requests.exceptions.RequestException as e:
            print(f"Ошибка соединения: {e}, попытка {attempt+1}")
        time.sleep(2)
    return None


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
        if not books:
            break

        all_books.extend(books)
        print(f"Собрано книг со страницы {page_num}: {len(books)}")

        page_num += 1
        time.sleep(random.uniform(*CONFIG["delay_range"]))

    return all_books
