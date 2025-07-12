from kafka import KafkaConsumer
import psycopg2
import json
import os
from dotenv import load_dotenv

load_dotenv()

consumer = KafkaConsumer(
    "user_events",
    bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS"),
    group_id="user-logins-consumer",
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

conn = psycopg2.connect(
    dbname=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
    host=os.getenv("DB_HOST"),
    port=os.getenv("DB_PORT")
)
cursor = conn.cursor()

cursor.execute("""
CREATE TABLE IF NOT EXISTS user_logins (
    id SERIAL PRIMARY KEY,
    username TEXT,
    event_type TEXT,
    event_time TIMESTAMP,
    sent_to_kafka BOOLEAN DEFAULT FALSE
)
""")
conn.commit()

for message in consumer:
    try:
        data = message.value
        print("Received:", data)

        cursor.execute("""
            INSERT INTO user_logins (username, event_type, event_time)
            VALUES (%s, %s, to_timestamp(%s))
        """, (data["user"], data["event"], data["timestamp"]))

        conn.commit()
    except Exception as e:
        print(f"Error processing message: {e}")
