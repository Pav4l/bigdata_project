from pymongo import MongoClient
from datetime import datetime, timedelta
import json

# Подключение к MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client["my_database"]
user_events = db["user_events"]
archived_users = db["archived_users"]

# Текущая дата
today = datetime.today()

# Пороговые даты
registration_cutoff = today - timedelta(days=30)
activity_cutoff = today - timedelta(days=14)

# Получение всех уникальных пользователей
all_users = user_events.aggregate([
    {
        "$group": {
            "_id": "$user_id",
            "last_event": {"$max": "$event_time"},
            "registration_date": {"$first": "$user_info.registration_date"},
            "email": {"$first": "$user_info.email"}
        }
    },
    {
        "$match": {
            "registration_date": {"$lt": registration_cutoff},
            "last_event": {"$lt": activity_cutoff}
        }
    }
])

archived_ids = []

for user in all_users:
    archived_user_doc = {
        "user_id": user["_id"],
        "email": user["email"],
        "registration_date": user["registration_date"],
        "last_event": user["last_event"],
        "archived_at": today
    }
    archived_users.insert_one(archived_user_doc)
    archived_ids.append(user["_id"])

# Создание отчета
report = {
    "date": today.strftime("%Y-%m-%d"),
    "archived_users_count": len(archived_ids),
    "archived_user_ids": archived_ids
}

# Сохранение отчета
filename = f"{today.strftime('%Y-%m-%d')}.json"
with open(filename, "w", encoding="utf-8") as f:
    json.dump(report, f, indent=4)

print(f"✅ Архивация завершена. Архивировано пользователей: {len(archived_ids)}")
print(f"📄 Отчет сохранен как: {filename}")
