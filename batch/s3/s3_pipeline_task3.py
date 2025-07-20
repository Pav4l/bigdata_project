# -*- coding: utf-8 -*-

import asyncio
import os
import shutil
import logging
from pathlib import Path
import pandas as pd
import aiofiles
from watchfiles import awatch
from dotenv import load_dotenv
from aiobotocore.session import get_session
from contextlib import asynccontextmanager

load_dotenv()

WATCH_DIR = Path("watched")
ARCHIVE_DIR = Path("archive")
LOG_FILE = Path("pipeline.log")

WATCH_DIR.mkdir(exist_ok=True)
ARCHIVE_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)


class AsyncS3:
    def __init__(self):
        self._auth = {
            "aws_access_key_id": os.getenv("key_id"),
            "aws_secret_access_key": os.getenv("secret_key"),
            "endpoint_url": "https://s3.ru-7.storage.selcloud.ru",
        }
        self._bucket = os.getenv("container")
        self._session = get_session()

    @asynccontextmanager
    async def _connect(self):
        async with self._session.create_client("s3", **self._auth) as conn:
            yield conn

    async def upload_file(self, path: Path):
        async with self._connect() as remote:
            async with aiofiles.open(path, "rb") as f:
                data = await f.read()
                await remote.put_object(Bucket=self._bucket, Key=path.name, Body=data)
        logging.info(f"✅ Uploaded: {path.name}")

    async def upload_log(self):
        if LOG_FILE.exists():
            async with self._connect() as remote:
                async with aiofiles.open(LOG_FILE, "rb") as f:
                    data = await f.read()
                    await remote.put_object(Bucket=self._bucket, Key=LOG_FILE.name, Body=data)
            logging.info("📝 Log uploaded")


async def process_file(file_path: Path):
    try:
        logging.info(f"📂 Processing: {file_path.name}")
        df = pd.read_csv(file_path)
        filtered = df[df.columns[:2]]
        temp_file = file_path.with_name(f"filtered_{file_path.name}")
        filtered.to_csv(temp_file, index=False)

        s3 = AsyncS3()
        await s3.upload_file(temp_file)
        await s3.upload_log()

        shutil.move(file_path, ARCHIVE_DIR / file_path.name)
        logging.info(f"📦 File moved to archive: {ARCHIVE_DIR / file_path.name}")

        temp_file.unlink()
        logging.info(f"🧹 Temporary file removed: {temp_file.name}")

    except Exception as e:
        logging.error(f"❌ Error processing {file_path.name}: {e}")


async def watch_folder():
    logging.info("👀 Watching 'watched/' folder for new CSV files...")
    async for changes in awatch(WATCH_DIR):
        for change, path_str in changes:
            path = Path(path_str)
            if path.suffix == ".csv" and path.parent == WATCH_DIR:
                await process_file(path)


async def process_existing_files():
    logging.info("🔎 Checking for existing CSV files in 'watched/'...")
    for file in WATCH_DIR.glob("*.csv"):
        await process_file(file)


async def main():
    await process_existing_files()
    await watch_folder()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("🚫 Canceled by user")
        print("\n🚫 Stop by Ctrl+C")
