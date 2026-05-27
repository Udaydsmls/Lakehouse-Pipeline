WITH source AS (
    SELECT * FROM {{ source('external', 'user_sessions') }}
),

cleaned AS (
    SELECT
        CAST(session_id AS VARCHAR)                  AS session_id,
        CAST(user_id AS VARCHAR)                     AS user_id,
        DATE(session_start)                          AS session_date,
        CAST(session_start AS TIMESTAMP_NTZ)        AS session_start,
        CAST(session_end AS TIMESTAMP_NTZ)          AS session_end,
        CAST(duration_seconds AS INT)                AS duration_seconds,
        CAST(pages_viewed AS INT)                    AS pages_viewed,
        CAST(products_viewed AS INT)                 AS products_viewed,
        CAST(searches AS INT)                        AS searches,
        CAST(add_to_cart_count AS INT)               AS add_to_cart_count,
        CAST(checkout_started AS BOOLEAN)            AS checkout_started,
        CAST(purchased AS BOOLEAN)                   AS purchased,
        CAST(outcome AS VARCHAR)                     AS outcome,
        CAST(device_type AS VARCHAR)                 AS device_type,
        CAST(country AS VARCHAR)                     AS country
    FROM source
)

SELECT * FROM cleaned
