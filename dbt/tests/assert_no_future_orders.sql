-- Orders should never be dated in the future. If any are, something is wrong
-- with the generator or the clock on the container.

SELECT
    order_id,
    ordered_at
FROM {{ ref('fct_orders') }}
WHERE ordered_at > CURRENT_TIMESTAMP
