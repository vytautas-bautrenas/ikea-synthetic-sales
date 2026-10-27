{{ config(
    materialized = 'table',
    cluster_by=['order_id'],
    sql_header = p_rand_gen_header()
) }}


{% set pg = var('p_granularity') %}


WITH lines AS (
    SELECT
        b.order_id,
        ln AS line_number,
        P_RAND_GEN(
            CONCAT(
                CAST(b.order_id AS STRING), '-',
                CAST(ln AS STRING)
            ),
            {{ pg }}
        ) AS slot
    FROM {{ ref('int_order_sizes') }} AS b
    CROSS JOIN UNNEST(GENERATE_ARRAY(1, b.basket_size)) AS ln
),

priced AS (
    SELECT
        l.order_id,
        l.line_number,
        p.product_id,
        p.unit_price_100,
        CAST(
            ROUND(
                p.unit_price_100 * p.vat_rate_100 / (100 + p.vat_rate_100)
            ) AS INT64
        ) AS line_vat_100
    FROM lines AS l
    INNER JOIN {{ ref('int_order_products') }} AS p
        ON l.slot = p.slot
)

SELECT
    order_id,
    line_number,
    product_id,
    1 AS line_quantity,
    unit_price_100 AS line_price_100,
    unit_price_100 AS line_total_100,
    line_vat_100,
    unit_price_100 - line_vat_100 AS line_total_excl_vat_100
FROM priced
