WITH source AS (
    SELECT * FROM {{ source('raw', 'users') }}
),

cleaned AS (
    SELECT
        CAST(user_id AS VARCHAR)                                         AS user_id,
        LOWER(TRIM(email))                                               AS email,
        SPLIT_PART(LOWER(TRIM(email)), '@', 2)                          AS email_domain,
        UPPER(TRIM(country))                                             AS country,
        LOWER(COALESCE(acquisition_channel, 'unknown'))                  AS acquisition_channel,
        LOWER(COALESCE(segment, 'new'))                                  AS segment,
        CAST(email_verified AS BOOLEAN)                                  AS is_email_verified,
        CAST(created_at AS TIMESTAMP_NTZ)                               AS registered_at,
        CAST(updated_at AS TIMESTAMP_NTZ)                               AS last_updated_at,
        DATEDIFF('day', CAST(created_at AS TIMESTAMP_NTZ), CURRENT_TIMESTAMP()) AS days_since_registration
    FROM source
)

SELECT * FROM cleaned
