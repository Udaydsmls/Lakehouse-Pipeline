WITH source AS (
    SELECT * FROM {{ source('ecommerce', 'products') }}
)

SELECT
    product_id,
    TRIM(name)                                  AS product_name,
    LOWER(TRIM(category))                       AS category,
    LOWER(TRIM(COALESCE(subcategory, 'general'))) AS subcategory,
    INITCAP(TRIM(COALESCE(brand, 'unknown')))   AS brand,
    base_price::DECIMAL(18, 2)                  AS base_price,
    COALESCE(is_active, TRUE)                   AS is_active,
    created_at::TIMESTAMP                       AS created_at
FROM source
