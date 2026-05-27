SELECT
    date,
    overall_conversion_rate
FROM {{ ref('mart_conversion_funnel') }}
WHERE overall_conversion_rate < 0.005
   OR overall_conversion_rate > 0.15
