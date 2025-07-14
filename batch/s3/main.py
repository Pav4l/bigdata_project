import asyncio
import os

from pathlib import Path
# Для создания асинхронного контекстного менеджера
from contextlib import asynccontextmanager
# Асинхронная версия boto3
from aiobotocore.session import get_session
# Ошибки при обращении к API
# from botocore.exceptions import ClientError
from dotenv import load_dotenv

load_dotenv()


# Получить путь до файла рядом с main.py
base_dir = Path(__file__).parent
file_path = base_dir / "big_file.csv"
output_path = base_dir / "downloaded_big_file.csv"


class AsyncObjectStorage:
    def __init__(self, *, key_id: str, secret: str, endpoint: str, container: str):
        self._auth = {
            "aws_access_key_id": key_id,
            "aws_secret_access_key": secret,
            "endpoint_url": endpoint,
        }
        self._bucket = container
        self._session = get_session()

    @asynccontextmanager
    async def _connect(self):
        async with self._session.create_client("s3", **self._auth) as connection:
            yield connection

    async def fetch_file(self, remote_name: str, local_target: str):
        async with self._connect() as remote:
            response = await remote.get_object(Bucket=self._bucket, Key=remote_name)
            body = await response["Body"].read()
            with open(local_target, "wb") as out:
                out.write(body)

    async def remove_file(self, remote_name: str):
        async with self._connect() as remote:
            await remote.delete_object(Bucket=self._bucket, Key=remote_name)

    async def send_file(self, local_path: str):
        filename = os.path.basename(local_path)
        async with self._connect() as remote:
            with open(local_path, "rb") as f:
                await remote.put_object(Bucket=self._bucket, Key=filename, Body=f)


async def run_demo():
    storage = AsyncObjectStorage(
        key_id=os.getenv("key_id"),
        secret=os.getenv("secret_key"),
        endpoint="https://s3.ru-7.storage.selcloud.ru",
        container=os.getenv("container")
    )
    await storage.send_file(str(file_path))
    await storage.fetch_file("big_file.csv", str(output_path))
    await storage.remove_file("big_file.csv")


if __name__ == "__main__":
    asyncio.run(run_demo())
