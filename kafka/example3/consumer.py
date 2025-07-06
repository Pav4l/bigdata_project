from kafka import KafkaConsumer
from clickhouse_connect import get_client
import json

consumer = KafkaConsumer(
    'sales_topic',
    bootstrap_servers='localhost:9092',
    group_id='etl_sales',
    auto_offset_reset='earliest',
    enable_auto_commit=False,
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

clickhouse = get_client(host='localhost', port=8123, username='user', password='strongpassword')
batch = []
import_info = {}

for msg in consumer:
    record = msg.value

    if record.get("type") == "import_end":
        import_info = record
        print(f"📦 Завершение импорта: {import_info}")
        break

    batch.append(record)

# Вставка в основную таблицу
columns = ['id', 'product', 'amount', 'region']
rows = [[rec['id'], rec['product'], rec['amount'], rec['region']] for rec in batch]

clickhouse.insert('customer.sales', rows, column_names=columns)


# Фиксация импорта
clickhouse.command(f"""
    INSERT INTO customer.imports
    (table_name, last_import_date, load_timestamp, row_count, comment)
    VALUES (
        '{import_info["table"]}',
        toDate('{import_info["import_date"]}'),
        now(),
        {import_info["row_count"]},
        'Импорт завершён через Kafka'
    )
""")

print("✅ Данные загружены и записаны в customer.imports")
consumer.commit()
