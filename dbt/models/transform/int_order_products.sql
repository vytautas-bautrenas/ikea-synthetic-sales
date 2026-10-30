{{ config(
    materialized = 'table',
    cluster_by = ['slot']
) }}

{% set pg = var('p_granularity') %}
{% set nrp_weight = var('non_rated_product_weight') %}

WITH prod AS (
    SELECT
        product_id,
        unit_price_100,
        unit_vat_100,
        unit_price_excl_vat_100,
        CAST(ROUND(vat_rate * 100) AS INT64) AS vat_rate_100,
        COALESCE(product_rating_count, {{ nrp_weight }}) AS weight
    FROM {{ ref('dim_products') }}
),

cdf_bounds AS (
    SELECT
        product_id,
        unit_price_100,
        unit_vat_100,
        unit_price_excl_vat_100,
        vat_rate_100,
        SUM(weight) OVER (ORDER BY product_id) / SUM(weight) OVER () AS cum,
        COALESCE(
            SUM(weight) OVER (
                ORDER BY product_id
                ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
            ),
            0
        ) / SUM(weight) OVER () AS prev
    FROM prod
)

SELECT
    product_id,
    unit_price_100,
    unit_vat_100,
    unit_price_excl_vat_100,
    vat_rate_100,
    slot
FROM cdf_bounds,
    UNNEST(GENERATE_ARRAY(
        CAST(FLOOR(prev * {{ pg }}) AS INT64),
        CAST(FLOOR(cum * {{ pg }}) AS INT64) - 1
    )) AS slot
