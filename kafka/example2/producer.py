# producer_pg_to_kafka.py
import psycopg2
from kafka import KafkaProducer
import json
import time
import os
from dotenv import load_dotenv

load_dotenv()

producer = KafkaProducer(
    bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS"),
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
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
    SELECT id, username, event_type, event_time, extract(epoch FROM event_time)
    FROM user_logins
    WHERE sent_to_kafka = FALSE OR sent_to_kafka IS NULL;
""")
rows = cursor.fetchall()

for row in rows:
    row_id, username, event_type, event_time, timestamp = row
    data = {
        "user": username,
        "event": event_type,
        "timestamp": float(timestamp)
    }

    producer.send("user_events", value=data)
    print("Sent:", data)

    cursor.execute("""
        UPDATE user_logins
        SET sent_to_kafka = TRUE
        WHERE id = %s
    """, (row_id,))

    time.sleep(0.5)

conn.commit()
cursor.close()
conn.close()
producer.flush()
