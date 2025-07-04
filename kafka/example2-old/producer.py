from kafka import KafkaProducer
import json
import time
import random

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

users = ["alice", "bob", "carol", "dave"]

while True:
    data = {
        "user": random.choice(users),
        "event": "login",
        "timestamp": time.time()
    }
    producer.send("user_events", value=data)
    print("Sent:", data)
    time.sleep(1)
