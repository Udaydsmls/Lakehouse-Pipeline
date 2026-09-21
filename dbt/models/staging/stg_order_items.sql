WITH source AS (
    SELECT * FROM {{ source('ecommerce', 'order_items') }}
)

SELECT
    order_item_id,
    order_id,
    product_id,
    quantity,
    unit_price::DECIMAL(18, 2)                  AS unit_price,
    (quantity * unit_price)::DECIMAL(18, 2)     AS line_total
FROM source
