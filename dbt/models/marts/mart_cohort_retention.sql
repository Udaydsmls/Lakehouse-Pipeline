WITH first_orders AS (
    SELECT
        user_id,
        DATE_TRUNC('month', MIN(ordered_at)) AS first_order_month
    FROM {{ ref('fct_orders') }}
    WHERE NOT is_cancelled
    GROUP BY user_id
),

user_orders AS (
    SELECT DISTINCT
        user_id,
        DATE_TRUNC('month', ordered_at) AS order_month
    FROM {{ ref('fct_orders') }}
    WHERE NOT is_cancelled
),

cohort_activity AS (
    SELECT
        fo.user_id,
        fo.first_order_month,
        uo.order_month,
        DATEDIFF('month', fo.first_order_month, uo.order_month) AS months_since_acquisition
    FROM first_orders fo
    JOIN user_orders uo ON fo.user_id = uo.user_id
    WHERE uo.order_month >= fo.first_order_month
),

cohort_sizes AS (
    SELECT
        first_order_month,
        COUNT(DISTINCT user_id) AS cohort_size
    FROM first_orders
    GROUP BY first_order_month
)

SELECT
    ca.first_order_month                        AS cohort_month,
    ca.months_since_acquisition,
    cs.cohort_size,
    COUNT(DISTINCT ca.user_id)                  AS retained_users,
    ROUND(COUNT(DISTINCT ca.user_id) / cs.cohort_size, 4) AS retention_rate
FROM cohort_activity ca
JOIN cohort_sizes cs ON ca.first_order_month = cs.first_order_month
GROUP BY
    ca.first_order_month,
    ca.months_since_acquisition,
    cs.cohort_size
ORDER BY
    ca.first_order_month,
    ca.months_since_acquisition
