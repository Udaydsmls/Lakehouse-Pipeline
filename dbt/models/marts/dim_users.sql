SELECT
    u.user_id,
    u.email,
    u.email_domain,
    u.country,
    u.acquisition_channel,
    u.segment,
    u.is_email_verified,
    u.registered_at,
    u.days_since_registration,
    COALESCE(f.purchase_count_180d, 0)      AS purchase_count_180d,
    COALESCE(f.total_spend_180d, 0)         AS total_spend_180d,
    f.avg_order_value_180d,
    f.days_since_last_purchase,
    f.days_since_last_visit,
    f.favourite_category,
    f.rfm_segment,
    COALESCE(f.ltv_estimate, 0)             AS ltv_estimate,
    CURRENT_TIMESTAMP()                     AS dbt_updated_at
FROM {{ ref('stg_users') }} u
LEFT JOIN {{ source('external', 'user_features') }} f
    ON u.user_id = f.user_id
