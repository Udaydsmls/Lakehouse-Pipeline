SELECT
    order_id,
    ordered_at
FROM {{ ref('fct_orders') }}
WHERE ordered_at > CURRENT_TIMESTAMP()
