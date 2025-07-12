# Пайплайн: PostgreSQL → Kafka → ClickHouse

## Описание

Этот проект реализует передачу событий пользователей (`user_logins`) из PostgreSQL в ClickHouse через Kafka.
Используется защита от повторной отправки данных с помощью логического поля `sent_to_kafka`.

## Запуск 
 
0. Запуск инфраструктуры

```bash
docker-compose up -d
```

1. Проверим, что создана таблица user_logins в БД PostgreSQL.

```sql
CREATE TABLE IF NOT EXISTS user_logins (
    id SERIAL PRIMARY KEY,
    username TEXT,
    event_type TEXT,
    event_time TIMESTAMP,
    sent_to_kafka BOOLEAN DEFAULT FALSE
)
```

Заполним таблицу user_logins.

```sql
INSERT INTO user_logins (username, event_type, event_time, sent_to_kafka)
VALUES
('alice', 'login', now(), False),
('bob', 'register', now(), False);
```

Проверим, что добавлен флаг sent_to_kafka в таблице user_logins БД PostgreSQL.

```sql
ALTER TABLE user_logins ADD COLUMN IF NOT EXISTS sent_to_kafka BOOLEAN DEFAULT FALSE;
```

Если таблица уже содержит какие-то записи, то пометим их как неотправленные.

```sql
UPDATE user_logins SET sent_to_kafka = FALSE;
```

2. Выполним запуск producer - данные из БД PostgreSQL в Kafka должны уйти.

```bash
python producer.py
```

3. Выполним запуск producer еще раз - данные из БД PostgreSQL в Kafka повторно не уходят.

```bash
python producer.py
```

3. Выполним запуск consumer - данные из Kafka записались в ClickHouse.

```bash
python consumer.py
```

-----

## Тест с "чистого листа"
(Для повторного тестирования)

1. Очистим таблицу user_logins в БД ClickHouse.

```sql
TRUNCATE TABLE user_logins;
```

2. В БД PostgreSQL сбросим флаги в таблице user_logins:

```sql
UPDATE user_logins SET sent_to_kafka = FALSE;
```

3. Теперь снова запустим producer - данные из БД PostgreSQL в Kafka должны уйти.

```bash
python producer.py
```

4. Выполним запуск producer еще раз - данные из БД PostgreSQL в Kafka повторно не уходят.

```bash
python producer.py
```

5. Выполним запуск consumer - данные из Kafka записались в ClickHouse.

```bash
python consumer.py
```