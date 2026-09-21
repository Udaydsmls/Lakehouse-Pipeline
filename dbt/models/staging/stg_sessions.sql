WITH source AS (
    SELECT * FROM {{ source('analytics', 'user_sessions') }}
)

SELECT
    session_id,
    user_id,
    session_date,
    session_start::TIMESTAMP    AS session_start,
    session_end::TIMESTAMP      AS session_end,
    duration_seconds,
    pages_viewed,
    products_viewed,
    searches,
    add_to_cart_count,
    checkout_started,
    purchased,
    outcome,
    device_type,
    country
FROM source
