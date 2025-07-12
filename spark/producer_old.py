from kafka import KafkaProducer
import json
import time
import random

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

while True:
    data = {"value": random.randint(1, 100)}
    print("Sending:", data)
    producer.send("test-topic", value=data)
    time.sleep(1)
