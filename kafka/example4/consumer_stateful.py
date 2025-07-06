from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'my_topic',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='latest',        # только новые сообщения, если нет offset-а
    enable_auto_commit=True,           # сохраняет offset автоматически
    group_id='my-consumer-group',      # имя группы сохраняется в Kafka
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

print("🧠 Stateful consumer (читает только новые):")
for message in consumer:
    print(message.value)
