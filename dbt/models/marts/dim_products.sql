SELECT
    p.product_id,
    p.name                                                  AS product_name,
    p.category,
    p.subcategory,
    p.brand,
    p.base_price,
    p.is_active,
    p.created_at,
    COALESCE(perf.total_views_30d, 0)                      AS total_views_30d,
    COALESCE(perf.total_add_to_cart_30d, 0)                AS total_add_to_cart_30d,
    COALESCE(perf.avg_cart_abandonment_rate_30d, 0)        AS avg_cart_abandonment_rate_30d,
    COALESCE(perf.unique_users_30d, 0)                     AS unique_users_30d
FROM {{ ref('stg_products') }} p
LEFT JOIN (
    SELECT
        product_id,
        SUM(total_views)                    AS total_views_30d,
        SUM(total_add_to_cart)              AS total_add_to_cart_30d,
        AVG(avg_cart_abandonment_rate)      AS avg_cart_abandonment_rate_30d,
        SUM(unique_users)                   AS unique_users_30d
    FROM {{ source('external', 'product_performance') }}
    WHERE date >= DATEADD('day', -30, CURRENT_DATE())
    GROUP BY product_id
) perf ON p.product_id = perf.product_id
