import boto3
from pathlib import Path
from botocore.exceptions import BotoCoreError, ClientError


class SyncObjectStorage:
    """S3-совместимое хранилище (Selectel)."""
    def __init__(self, *, key_id: str, secret: str, endpoint: str, container: str):
        if not key_id or not secret:
            raise RuntimeError("AWS credentials are required (key_id, secret_key).")

        self._client = boto3.client(
            "s3",
            aws_access_key_id=key_id,
            aws_secret_access_key=secret,
            endpoint_url=endpoint
        )
        self._bucket = container

    def upload_file(self, local_path: str, remote_name: str | None = None):
        """Загружает файл в хранилище."""
        if remote_name is None:
            remote_name = Path(local_path).name

        try:
            self._client.upload_file(Filename=local_path, Bucket=self._bucket, Key=remote_name)
            print(f"✅ Uploaded {local_path} -> s3://{self._bucket}/{remote_name}")
        except (BotoCoreError, ClientError) as e:
            print(f"Ошибка при загрузке: {e}")
            raise
