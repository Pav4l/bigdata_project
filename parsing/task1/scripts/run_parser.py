import pandas as pd
from parser.fetcher import crawl_site
from storage.s3_storage import SyncObjectStorage
from utils.config import OUTPUT_DIR, get_env


def save_to_csv(data: list[dict], filename: str = "books.csv"):
    """Сохраняет данные в CSV и выводит статистику."""
    df = pd.DataFrame(data)
    output_path = OUTPUT_DIR / filename
    df.to_csv(output_path, index=False, encoding="utf-8")
    print(f"💾 Данные сохранены в {output_path}")

    print("Всего записей:", len(df))
    print("Уникальных книг:", df['title'].nunique())
    print(df['price'].value_counts().head())

    return output_path


def upload_to_selectel(local_file: str):
    storage = SyncObjectStorage(
        key_id=get_env("key_id"),
        secret=get_env("secret_key"),
        endpoint="https://s3.ru-7.storage.selcloud.ru",
        container="data-engineer-practice-pavel3",
    )
    storage.upload_file(local_file)


if __name__ == "__main__":
    books_data = crawl_site()
    csv_path = save_to_csv(books_data)
    upload_to_selectel(str(csv_path))
