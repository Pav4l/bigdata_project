-- 1. Сырая таблица user_events (с TTL на 30 дней)
CREATE TABLE user_events
(
    user_id UInt32,
    event_type String,
    points_spent UInt32,
    event_time DateTime
)
ENGINE = MergeTree()
ORDER BY (event_time, user_id)
TTL event_time + INTERVAL 30 DAY;

-- 2. Вставка тестовых данных
INSERT INTO user_events VALUES
(1, 'login', 0, now() - INTERVAL 10 DAY),
(2, 'signup', 0, now() - INTERVAL 10 DAY),
(3, 'login', 0, now() - INTERVAL 10 DAY),
(1, 'login', 0, now() - INTERVAL 7 DAY),
(2, 'login', 0, now() - INTERVAL 7 DAY),
(3, 'purchase', 30, now() - INTERVAL 7 DAY),
(1, 'purchase', 50, now() - INTERVAL 5 DAY),
(2, 'logout', 0, now() - INTERVAL 5 DAY),
(4, 'login', 0, now() - INTERVAL 5 DAY),
(1, 'login', 0, now() - INTERVAL 3 DAY),
(3, 'purchase', 70, now() - INTERVAL 3 DAY),
(5, 'signup', 0, now() - INTERVAL 3 DAY),
(2, 'purchase', 20, now() - INTERVAL 1 DAY),
(4, 'logout', 0, now() - INTERVAL 1 DAY),
(5, 'login', 0, now() - INTERVAL 1 DAY),
(1, 'purchase', 25, now()),
(2, 'login', 0, now()),
(3, 'logout', 0, now()),
(6, 'signup', 0, now()),
(6, 'purchase', 100, now());

-- 3. Агрегированная таблица (с TTL на 180 дней)
CREATE TABLE user_events_agg
(
    event_date Date,
    event_type String,
    unique_users_state AggregateFunction(uniq, UInt32),
    total_spent_state AggregateFunction(sum, UInt32),
    total_actions_state AggregateFunction(count)
)
ENGINE = AggregatingMergeTree()
ORDER BY (event_date, event_type)
TTL event_date + INTERVAL 180 DAY;

-- 4. Materialized View с state функциями
CREATE MATERIALIZED VIEW user_events_mv
TO user_events_agg AS
SELECT
    toDate(event_time) AS event_date,
    event_type,
    uniqState(user_id) AS unique_users_state,
    sumState(points_spent) AS total_spent_state,
    countState() AS total_actions_state
FROM user_events
GROUP BY event_date, event_type;

-- 5. Быстрая аналитика: мерджим агрегаты (с использованием merge-функций)
SELECT
    event_date,
    event_type,
    uniqMerge(unique_users_state) AS unique_users,
    sumMerge(total_spent_state) AS total_spent,
    countMerge(total_actions_state) AS total_actions
FROM user_events_agg
GROUP BY event_date, event_type
ORDER BY event_date, event_type;

-- 6. Retention: вернувшиеся пользователи через 7 дней
-- Считаем retention от signup события

SELECT
    s.signup_date AS total_users_day_0,
    countDistinct(s.user_id) AS total_users,
    countDistinct(r.user_id) AS returned_in_7_days,
    round(100.0 * countDistinct(r.user_id) / countDistinct(s.user_id), 2) AS retention_7d_percent
FROM
    (
        SELECT 
            toDate(event_time) AS signup_date,
            user_id
        FROM user_events
        WHERE event_type = 'signup'
    ) AS s
LEFT JOIN
    (
        SELECT 
            user_id,
            toDate(event_time) AS return_date
        FROM user_events
        WHERE event_type IN ('login', 'purchase', 'logout')
    ) AS r
    ON s.user_id = r.user_id
    AND r.return_date > s.signup_date
    AND r.return_date <= s.signup_date + 7
GROUP BY s.signup_date
ORDER BY s.signup_date;