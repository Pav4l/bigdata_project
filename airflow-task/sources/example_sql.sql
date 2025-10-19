-- SCHEDULE: 30 3 * * *    -- cron через комментарий (строка после "SCHEDULE:")
-- CONN_ID: pg_extra       -- опциональный connection id, если не указан, используется postgres_default

CREATE TABLE IF NOT EXISTS test_table (
    id serial PRIMARY KEY,
    name text
);

INSERT INTO test_table (name) VALUES ('alpha');

ALTER TABLE test_table ADD COLUMN created_at timestamp DEFAULT now();
