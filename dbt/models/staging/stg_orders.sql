WITH source AS (
    SELECT * FROM {{ source('raw', 'orders') }}
    WHERE _airbyte_normalized_at IS NOT NULL
),

cleaned AS (
    SELECT
        CAST(order_id AS VARCHAR)                                      AS order_id,
        CAST(user_id AS VARCHAR)                                       AS user_id,
        LOWER(TRIM(status))                                            AS status,
        CAST(total_amount AS DECIMAL(18, 2))                          AS total_amount,
        COALESCE(CAST(discount_amount AS DECIMAL(18, 2)), 0)          AS discount_amount,
        CAST(total_amount AS DECIMAL(18, 2))
            - COALESCE(CAST(discount_amount AS DECIMAL(18, 2)), 0)    AS net_amount,
        LOWER(TRIM(payment_method))                                    AS payment_method,
        UPPER(TRIM(shipping_address_country))                          AS shipping_country,
        CAST(created_at AS TIMESTAMP_NTZ)                             AS ordered_at,
        CAST(updated_at AS TIMESTAMP_NTZ)                             AS updated_at,
        CASE WHEN LOWER(TRIM(status)) = 'cancelled'
             THEN TRUE ELSE FALSE END                                  AS is_cancelled,
        CASE WHEN LOWER(TRIM(status)) IN ('return_requested', 'return_completed')
             THEN TRUE ELSE FALSE END                                  AS is_returned
    FROM source
)

SELECT * FROM cleaned
