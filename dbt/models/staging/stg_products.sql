WITH source AS (
    SELECT * FROM {{ source('raw', 'products') }}
),

cleaned AS (
    SELECT
        CAST(product_id AS VARCHAR)                              AS product_id,
        TRIM(name)                                               AS name,
        LOWER(TRIM(category))                                    AS category,
        LOWER(TRIM(COALESCE(subcategory, 'general')))            AS subcategory,
        INITCAP(TRIM(COALESCE(brand, 'Unknown')))                AS brand,
        CAST(base_price AS DECIMAL(18, 2))                      AS base_price,
        CAST(COALESCE(is_active, TRUE) AS BOOLEAN)              AS is_active,
        CAST(created_at AS TIMESTAMP_NTZ)                       AS created_at
    FROM source
)

SELECT * FROM cleaned
