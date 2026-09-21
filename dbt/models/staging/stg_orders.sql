WITH source AS (
    SELECT * FROM {{ source('ecommerce', 'orders') }}
)

SELECT
    order_id,
    user_id,
    LOWER(TRIM(status))                                     AS status,
    total_amount::DECIMAL(18, 2)                            AS total_amount,
    COALESCE(discount_amount, 0)::DECIMAL(18, 2)            AS discount_amount,
    (total_amount - COALESCE(discount_amount, 0))::DECIMAL(18, 2) AS net_amount,
    LOWER(TRIM(payment_method))                             AS payment_method,
    UPPER(TRIM(shipping_country))                           AS shipping_country,
    created_at::TIMESTAMP                                   AS ordered_at,
    status = 'cancelled'                                    AS is_cancelled,
    status = 'returned'                                     AS is_returned
FROM source
