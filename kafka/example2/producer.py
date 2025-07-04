# producer_pg_to_kafka.py
import psycopg2
from kafka import KafkaProducer
import json
import time

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

conn = psycopg2.connect(
    dbname="test_db", user="admin", password="admin", host="localhost", port=5432
)
cursor = conn.cursor()

cursor.execute("""
    SELECT username, event_type, event_time, extract(epoch FROM event_time)
    FROM user_logins
    WHERE sent_to_kafka = FALSE OR sent_to_kafka IS NULL;
""")
rows = cursor.fetchall()

for row in rows:
    username, event_type, event_time, timestamp = row
    data = {
        "user": username,
        "event": event_type,
        "timestamp": float(timestamp)
    }

    producer.send("user_events", value=data)
    print("Sent:", data)

    # Помечаем как отправленное
    cursor.execute("""
        UPDATE user_logins
        SET sent_to_kafka = TRUE
        WHERE username = %s AND event_type = %s AND event_time = %s
    """, (username, event_type, event_time))

    time.sleep(0.5)

conn.commit()
cursor.close()
conn.close()
producer.flush()
