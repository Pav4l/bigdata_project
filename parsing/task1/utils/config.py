import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

CONFIG = {
    "base_url": "http://books.toscrape.com/catalogue/page-{}.html",
    "headers": {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"},
    "delay_range": (1, 3),
    "max_retries": 3,
}

OUTPUT_DIR = Path("output")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


# Удобный доступ к ключам
def get_env(var_name: str) -> str:
    value = os.getenv(var_name)
    if not value:
        raise RuntimeError(f"❌ Переменная окружения {var_name} не найдена")
    return value
