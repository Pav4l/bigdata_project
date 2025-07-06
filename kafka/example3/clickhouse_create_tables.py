from clickhouse_connect import get_client

client = get_client(host='localhost', port=8123, username='user', password='strongpassword')

# Создание базы
client.command("CREATE DATABASE IF NOT EXISTS customer")

# Таблица sales
client.command("""
CREATE TABLE IF NOT EXISTS customer.sales (
    id UInt64,
    product String,
    amount Float64,
    region String
) ENGINE = MergeTree
ORDER BY id
""")

# Таблица imports
client.command("""
CREATE TABLE IF NOT EXISTS customer.imports (
    table_name String,
    last_import_date Date,
    load_timestamp DateTime,
    row_count UInt64,
    comment String
) ENGINE = MergeTree
ORDER BY (table_name, last_import_date)
""")

print("✅ База и таблицы созданы")
