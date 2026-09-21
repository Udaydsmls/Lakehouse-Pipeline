WITH source AS (
    SELECT * FROM {{ source('ecommerce', 'users') }}
)

SELECT
    user_id,
    LOWER(TRIM(email))                              AS email,
    SPLIT_PART(LOWER(TRIM(email)), '@', 2)          AS email_domain,
    UPPER(TRIM(country))                            AS country,
    LOWER(COALESCE(acquisition_channel, 'unknown')) AS acquisition_channel,
    LOWER(COALESCE(segment, 'new'))                 AS segment,
    email_verified                                  AS is_email_verified,
    created_at::TIMESTAMP                           AS registered_at,
    (CURRENT_DATE - created_at::DATE)               AS days_since_registration
FROM source
