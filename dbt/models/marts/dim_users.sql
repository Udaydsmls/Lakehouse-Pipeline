-- User dimension: profile columns from Postgres plus the RFM features Spark
-- computed.

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
    COALESCE(f.order_count, 0)      AS order_count_180d,
    COALESCE(f.total_spend, 0)      AS total_spend_180d,
    f.avg_order_value               AS avg_order_value_180d,
    f.days_since_last_order,
    f.days_since_last_visit,
    f.session_count,
    f.bounce_rate,
    f.rfm_recency_score,
    f.rfm_frequency_score,
    f.rfm_monetary_score,
    f.rfm_segment,
    COALESCE(f.ltv_estimate, 0)     AS ltv_estimate
FROM {{ ref('stg_users') }} u
LEFT JOIN {{ source('analytics', 'user_features') }} f ON u.user_id = f.user_id
