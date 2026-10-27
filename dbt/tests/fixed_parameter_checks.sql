WITH counts AS (
    SELECT
        (SELECT COUNT(*) FROM {{ ref('fact_orders') }}) AS total_orders,
        (SELECT COUNT(*) FROM {{ ref('dim_customers') }}) AS total_customers
)
SELECT
    total_orders,
    {{ var('n_orders') }} AS expected_orders,
    total_customers,
    {{ var('n_customers') }} AS expected_customers
FROM counts
WHERE total_orders != {{ var('n_orders') }}
   OR total_customers != {{ var('n_customers') }}
