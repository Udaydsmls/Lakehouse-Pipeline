-- One row per order, with the line items rolled up into it.

WITH items AS (
    SELECT
        oi.order_id,
        COUNT(*)                                            AS item_count,
        COUNT(DISTINCT oi.product_id)                       AS distinct_product_count,
        SUM(oi.quantity)                                    AS total_quantity,
        COUNT(DISTINCT p.category)                          AS category_count,
        STRING_AGG(DISTINCT p.category, ', ' ORDER BY p.category) AS categories_purchased
    FROM {{ ref('stg_order_items') }} oi
    LEFT JOIN {{ ref('stg_products') }} p ON oi.product_id = p.product_id
    GROUP BY oi.order_id
)

SELECT
    o.order_id,
    o.user_id,
    o.ordered_at,
    o.ordered_at::DATE                      AS order_date,
    DATE_TRUNC('week', o.ordered_at)::DATE  AS order_week,
    DATE_TRUNC('month', o.ordered_at)::DATE AS order_month,
    o.status,
    o.payment_method,
    o.shipping_country,
    o.total_amount,
    o.discount_amount,
    o.net_amount,
    o.is_cancelled,
    o.is_returned,
    COALESCE(i.item_count, 0)               AS item_count,
    COALESCE(i.distinct_product_count, 0)   AS distinct_product_count,
    COALESCE(i.total_quantity, 0)           AS total_quantity,
    COALESCE(i.category_count, 0)           AS category_count,
    i.categories_purchased
FROM {{ ref('stg_orders') }} o
LEFT JOIN items i ON o.order_id = i.order_id
