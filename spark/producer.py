import json, random, time, uuid
from kafka import KafkaProducer
from datetime import datetime
from faker import Faker

fake = Faker()

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    linger_ms=5,
    batch_size=32768
)

categories = ['groceries', 'beverages', 'snacks', 'electronics']
items_by_category = {
    'groceries': ['apple', 'banana', 'bread', 'milk'],
    'beverages': ['water', 'juice', 'soda', 'coffee'],
    'snacks': ['chips', 'cookies', 'nuts'],
    'electronics': ['headphones', 'keyboard', 'mouse']
}

CHECKS_PER_SECOND = 5000
BATCH_SIZE = 500
ITERATIONS = CHECKS_PER_SECOND // BATCH_SIZE

def generate_check():
    items = []
    for _ in range(random.randint(1, 5)):
        category = random.choice(categories)
        item = {
            "item_id": str(random.randint(10000000, 99999999)),
            "category": category,
            "item_name": random.choice(items_by_category[category]),
            "price": round(random.uniform(20, 100), 2),
            "quantity": random.randint(1, 3)
        }
        items.append(item)

    return {
        "store_id": random.randint(100, 105),
        "store_name": fake.company(),
        "cashier_id": random.randint(1, 10),
        "cashier_name": fake.name(),
        "check_id": str(uuid.uuid4()),
        "timestamp": datetime.now().isoformat(),
        "items": items
    }

# ➤ Показываем первые 2 чека
for _ in range(2):
    sample = generate_check()
    print(json.dumps(sample, indent=2))

# ➤ Основной бесконечный поток
while True:
    start = time.time()
    for _ in range(ITERATIONS):
        for _ in range(BATCH_SIZE):
            check = generate_check()
            producer.send("receipts", value=check)
    producer.flush()
    elapsed = time.time() - start
    to_sleep = max(0, 1 - elapsed)
    print(f"Sent {CHECKS_PER_SECOND} checks in {elapsed:.4f}s")
    time.sleep(to_sleep)
