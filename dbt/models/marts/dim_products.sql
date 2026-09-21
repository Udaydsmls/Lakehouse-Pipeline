-- Product dimension with the last 30 days of engagement joined on.

WITH recent_performance AS (
    SELECT
        product_id,
        SUM(total_views)         AS views_30d,
        SUM(total_add_to_cart)   AS add_to_cart_30d,
        SUM(total_purchases)     AS purchases_30d,
        AVG(conversion_rate)     AS avg_conversion_rate_30d
    FROM {{ source('analytics', 'product_performance') }}
    WHERE date >= CURRENT_DATE - 30
    GROUP BY product_id
)

SELECT
    p.product_id,
    p.product_name,
    p.category,
    p.subcategory,
    p.brand,
    p.base_price,
    p.is_active,
    COALESCE(r.views_30d, 0)            AS views_30d,
    COALESCE(r.add_to_cart_30d, 0)      AS add_to_cart_30d,
    COALESCE(r.purchases_30d, 0)        AS purchases_30d,
    COALESCE(r.avg_conversion_rate_30d, 0) AS avg_conversion_rate_30d
FROM {{ ref('stg_products') }} p
LEFT JOIN recent_performance r ON p.product_id = r.product_id
