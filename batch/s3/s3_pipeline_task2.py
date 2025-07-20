import boto3
import os
from dotenv import load_dotenv
from botocore.exceptions import ClientError

load_dotenv()

s3 = boto3.client(
    "s3",
    aws_access_key_id=os.getenv("key_id"),
    aws_secret_access_key=os.getenv("secret_key"),
    endpoint_url="https://s3.ru-7.storage.selcloud.ru",
)

BUCKET = os.getenv("container")
LOCAL_FILE = "big_file.csv"
DOWNLOAD_FILE = "downloaded_big_file.csv"
OBJECT_KEY = "test_versioned_file.csv"


# 1. Проверяем статус версионирования и включаем, если выключено
try:
    versioning = s3.get_bucket_versioning(Bucket=BUCKET)
    status = versioning.get("Status", "")
    if status != "Enabled":
        s3.put_bucket_versioning(
            Bucket=BUCKET,
            VersioningConfiguration={"Status": "Enabled"}
        )
        print("✅ Versioning enabled")
    else:
        print("Versioning already included")
except ClientError as e:
    print(f"Error checking/enabling versioning: {e}")
    exit(1)


# 2. Загружаем файл несколько раз, чтобы создать версии
with open(LOCAL_FILE, "rb") as f:
    s3.put_object(Bucket=BUCKET, Key=OBJECT_KEY, Body=f)
print("Uploaded version 1")

with open(LOCAL_FILE, "rb") as f:
    s3.put_object(Bucket=BUCKET, Key=OBJECT_KEY, Body=f)
print("Uploaded version 2")


# 3. Получаем список версий объекта
try:
    versions = s3.list_object_versions(Bucket=BUCKET, Prefix=OBJECT_KEY)
    all_versions = versions.get("Versions", [])
    print(f"Found {len(all_versions)} versions")

    if len(all_versions) < 2:
        print("Not enough versions to download the previous one")
    else:
        previous_version_id = all_versions[1]["VersionId"]
        print(f"Downloading previous version id: {previous_version_id}")

        s3.download_file(BUCKET, OBJECT_KEY, DOWNLOAD_FILE, ExtraArgs={"VersionId": previous_version_id})
        print(f"Downloaded previous version to {DOWNLOAD_FILE}")

except ClientError as e:
    print(f"Error while working with versions: {e}")
