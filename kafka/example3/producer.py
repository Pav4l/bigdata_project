from kafka import KafkaProducer
import json

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

data = [
    {"id": 1, "product": "vodka", "amount": 500.0, "region": "Moscow"},
    {"id": 2, "product": "wine", "amount": 1200.5, "region": "Kazan"},
    {"id": 3, "product": "whisky", "amount": 3000.0, "region": "SPB"},
];

for record in data:
    producer.send('sales_topic', value=record)

# Завершающий маркер
producer.send('sales_topic', value={
    "type": "import_end",
    "table": "customer.sales",
    "import_date": "2025-05-21",
    "row_count": len(data)
})

producer.flush()
print("✅ Данные отправлены в Kafka")
