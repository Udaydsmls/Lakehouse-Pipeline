-- Monthly cohort retention: of the users who first ordered in month X, how
-- many came back and ordered again N months later.

WITH first_orders AS (
    SELECT
        user_id,
        DATE_TRUNC('month', MIN(ordered_at))::DATE AS cohort_month
    FROM {{ ref('fct_orders') }}
    WHERE NOT is_cancelled
    GROUP BY user_id
),

monthly_activity AS (
    SELECT DISTINCT
        user_id,
        DATE_TRUNC('month', ordered_at)::DATE AS order_month
    FROM {{ ref('fct_orders') }}
    WHERE NOT is_cancelled
),

cohort_sizes AS (
    SELECT
        cohort_month,
        COUNT(*) AS cohort_size
    FROM first_orders
    GROUP BY cohort_month
)

SELECT
    f.cohort_month,
    -- Whole months between the first order and this one.
    (DATE_PART('year', AGE(a.order_month, f.cohort_month)) * 12
        + DATE_PART('month', AGE(a.order_month, f.cohort_month)))::INT AS months_since_first_order,
    c.cohort_size,
    COUNT(DISTINCT a.user_id) AS retained_users,
    ROUND(COUNT(DISTINCT a.user_id)::NUMERIC / c.cohort_size, 4) AS retention_rate
FROM first_orders f
JOIN monthly_activity a ON f.user_id = a.user_id AND a.order_month >= f.cohort_month
JOIN cohort_sizes c ON f.cohort_month = c.cohort_month
GROUP BY f.cohort_month, months_since_first_order, c.cohort_size
ORDER BY f.cohort_month, months_since_first_order
