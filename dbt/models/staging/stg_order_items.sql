WITH source AS (
    SELECT * FROM {{ source('raw', 'order_items') }}
),

cleaned AS (
    SELECT
        CAST(order_item_id AS VARCHAR)                                            AS order_item_id,
        CAST(order_id AS VARCHAR)                                                 AS order_id,
        CAST(product_id AS VARCHAR)                                               AS product_id,
        CAST(quantity AS INT)                                                     AS quantity,
        CAST(unit_price AS DECIMAL(18, 2))                                       AS unit_price,
        CAST(quantity AS INT) * CAST(unit_price AS DECIMAL(18, 2))               AS line_total,
        COALESCE(CAST(discount_percentage AS DECIMAL(5, 4)), 0)                  AS discount_pct,
        (CAST(quantity AS INT) * CAST(unit_price AS DECIMAL(18, 2)))
            * (1 - COALESCE(CAST(discount_percentage AS DECIMAL(5, 4)), 0))      AS discounted_line_total
    FROM source
)

SELECT * FROM cleaned
