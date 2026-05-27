SELECT
    o.order_id,
    o.user_id,
    o.ordered_at,
    DATE(o.ordered_at)                                                          AS order_date,
    DATE_TRUNC('week', o.ordered_at)                                            AS order_week,
    DATE_TRUNC('month', o.ordered_at)                                           AS order_month,
    o.status,
    o.payment_method,
    o.shipping_country,
    o.total_amount,
    o.discount_amount,
    o.net_amount,
    o.is_cancelled,
    o.is_returned,
    COUNT(DISTINCT oi.order_item_id)                                             AS item_count,
    COUNT(DISTINCT oi.product_id)                                               AS distinct_product_count,
    SUM(oi.quantity)                                                             AS total_quantity,
    COUNT(DISTINCT p.category)                                                  AS category_count,
    LISTAGG(DISTINCT p.category, ',') WITHIN GROUP (ORDER BY p.category)        AS categories_purchased
FROM {{ ref('stg_orders') }} o
LEFT JOIN {{ ref('stg_order_items') }} oi ON o.order_id = oi.order_id
LEFT JOIN {{ ref('stg_products') }} p ON oi.product_id = p.product_id
GROUP BY ALL
