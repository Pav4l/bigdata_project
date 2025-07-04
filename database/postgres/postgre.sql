-- 0. Создание таблиц
/*
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    name TEXT,
    email TEXT,
    role TEXT,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE users_audit (
    id SERIAL PRIMARY KEY,
    user_id INTEGER,
    changed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    changed_by TEXT,
    field_changed TEXT,
    old_value TEXT,
    new_value TEXT
);*/

-- 1. Функция логирования изменений
CREATE OR REPLACE FUNCTION log_user_changes()
RETURNS TRIGGER AS $$
BEGIN
  IF NEW.name IS DISTINCT FROM OLD.name THEN
    INSERT INTO users_audit(user_id, changed_by, field_changed, old_value, new_value)
    VALUES (OLD.id, current_user, 'name', OLD.name, NEW.name);
  END IF;

  IF NEW.email IS DISTINCT FROM OLD.email THEN
    INSERT INTO users_audit(user_id, changed_by, field_changed, old_value, new_value)
    VALUES (OLD.id, current_user, 'email', OLD.email, NEW.email);
  END IF;

  IF NEW.role IS DISTINCT FROM OLD.role THEN
    INSERT INTO users_audit(user_id, changed_by, field_changed, old_value, new_value)
    VALUES (OLD.id, current_user, 'role', OLD.role, NEW.role);
  END IF;

  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- 2. Триггер на обновление в таблице users
DROP TRIGGER IF EXISTS trg_log_user_changes ON users;

CREATE TRIGGER trg_log_user_changes
AFTER UPDATE ON users
FOR EACH ROW
WHEN (OLD.* IS DISTINCT FROM NEW.*)
EXECUTE FUNCTION log_user_changes();

-- 3. Установка расширения pg_cron
CREATE EXTENSION IF NOT EXISTS pg_cron;

-- 4. Функция экспорта свежих данных за текущий день
CREATE OR REPLACE FUNCTION export_today_audit_to_csv()
RETURNS void AS $$
DECLARE
  file_path TEXT;
BEGIN
  file_path := '/tmp/users_audit_export_' || TO_CHAR(CURRENT_DATE, 'YYYY_MM_DD') || '.csv';

  EXECUTE '
    COPY (
      SELECT user_id, changed_at, changed_by, field_changed, old_value, new_value
      FROM users_audit
      WHERE changed_at::date = CURRENT_DATE
    ) TO ' || quote_literal(file_path) || ' WITH CSV HEADER';
END;
$$ LANGUAGE plpgsql;

-- 5. Создание cron-задания на 3:00 ночи
SELECT cron.schedule(
  'daily_users_audit_export',
  '0 3 * * *',
  $$SELECT export_today_audit_to_csv();$$
);

-- 6. Проверки
-- Подготовка данных + изменение данных
/*
INSERT INTO users (name, email, role)
VALUES ('Иван Иванов', 'ivan@example.com', 'user'),
		('Сидр Сидоров', 'sidr@example.com', 'user');

UPDATE users
SET name = 'Иван Петров', email = 'petrov@example.com'
WHERE name = 'Иван Иванов';
commit;
*/

-- SQL
/*
SELECT * FROM cron.job;
SELECT * FROM users_audit ORDER BY changed_at DESC;

SELECT *
FROM users_audit
WHERE changed_at::date = CURRENT_DATE;
*/
-- в Docker:
--docker exec -it d6e6fd140f24 bash (d6e6fd140f24 - container_id для postgres_db)
--
-- скопировать файл из контейнера на хост:
--docker cp d6e6fd140f24:/tmp/users_audit_export_2025_06_21.csv ./users_audit_export_2025_06_21.csv