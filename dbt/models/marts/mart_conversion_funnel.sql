WITH daily_sessions AS (
    SELECT
        session_date,
        COUNT(*)                                                        AS total_sessions,
        COUNT(DISTINCT user_id)                                         AS unique_users,
        SUM(CASE WHEN add_to_cart_count > 0 THEN 1 ELSE 0 END)        AS sessions_with_cart,
        SUM(CASE WHEN checkout_started THEN 1 ELSE 0 END)              AS sessions_with_checkout,
        SUM(CASE WHEN purchased THEN 1 ELSE 0 END)                     AS sessions_with_purchase,
        SUM(CASE WHEN outcome = 'bounce' THEN 1 ELSE 0 END)            AS bounce_sessions,
        AVG(duration_seconds)                                           AS avg_session_duration_seconds,
        AVG(pages_viewed)                                               AS avg_pages_viewed
    FROM {{ ref('stg_sessions') }}
    GROUP BY session_date
),

daily_revenue AS (
    SELECT
        order_date,
        SUM(net_amount)             AS daily_revenue,
        COUNT(DISTINCT order_id)    AS daily_orders,
        COUNT(DISTINCT user_id)     AS purchasing_users,
        AVG(net_amount)             AS avg_order_value
    FROM {{ ref('fct_orders') }}
    WHERE NOT is_cancelled
    GROUP BY order_date
)

SELECT
    s.session_date                                                                      AS date,
    s.total_sessions,
    s.unique_users,
    s.sessions_with_cart,
    s.sessions_with_checkout,
    s.sessions_with_purchase,
    s.bounce_sessions,
    s.avg_session_duration_seconds,
    s.avg_pages_viewed,
    ROUND(s.sessions_with_cart / NULLIF(s.total_sessions, 0), 4)                       AS browse_to_cart_rate,
    ROUND(s.sessions_with_checkout / NULLIF(s.sessions_with_cart, 0), 4)               AS cart_to_checkout_rate,
    ROUND(s.sessions_with_purchase / NULLIF(s.sessions_with_checkout, 0), 4)           AS checkout_to_purchase_rate,
    ROUND(s.sessions_with_purchase / NULLIF(s.total_sessions, 0), 4)                   AS overall_conversion_rate,
    ROUND(s.bounce_sessions / NULLIF(s.total_sessions, 0), 4)                          AS bounce_rate,
    r.daily_revenue,
    r.daily_orders,
    r.purchasing_users,
    r.avg_order_value,
    ROUND(r.daily_revenue / NULLIF(s.unique_users, 0), 2)                              AS revenue_per_user
FROM daily_sessions s
LEFT JOIN daily_revenue r ON s.session_date = r.order_date
