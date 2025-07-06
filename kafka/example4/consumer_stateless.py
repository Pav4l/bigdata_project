from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'my_topic',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',  # всегда читать с самого начала
    enable_auto_commit=False,       # не сохраняет offset
    group_id=None,                  # нет группы — состояние не сохраняется
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

print("🧼 Stateless consumer (всегда читает с начала):")
for message in consumer:
    print(message.value)
