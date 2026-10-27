{{ config(
    materialized = 'ephemeral',
    cluster_by=['order_id'],
    sql_header = p_rand_gen_header()
) }}

{% set pg = var('p_granularity') %}
{% set online_share_1000 = var('online_share') * 1000 %}
{% set parcel_max_items = var('parcel_max_items') %}
{% set n_customers = var('n_customers') %}


WITH order_sizes AS (
    SELECT
        ios.order_id,
        ios.basket_size AS order_size,
        ln AS line_number,
        P_RAND_GEN(
            CONCAT(
                CAST(ios.order_id AS STRING), '-',
                CAST(ln AS STRING)
            ),
            {{ pg }}
        ) AS slot
    FROM {{ ref('int_order_sizes') }} AS ios
    CROSS JOIN UNNEST(GENERATE_ARRAY(1, ios.basket_size)) AS ln
),

order_products AS (
    SELECT
        os.order_id,
        os.order_size,
        op.unit_price_100,
        CAST(
            ROUND(
                op.unit_price_100 * op.vat_rate_100 / (100 + op.vat_rate_100)
            ) AS INT64
        ) AS line_vat_100
    FROM order_sizes AS os
    INNER JOIN {{ ref('int_order_products') }} AS op
        ON os.slot = op.slot
),

lines_agg AS (
    SELECT
        order_id,
        ANY_VALUE(order_size) AS order_size,
        SUM(unit_price_100) AS total_goods_100,
        SUM(line_vat_100) AS total_goods_vat_100,
        ROUND(
            SAFE_DIVIDE(
                SUM(line_vat_100) * 100,
                SUM(unit_price_100 - line_vat_100)
            ),
            0
        ) AS order_vat_rate_100
    FROM order_products
    GROUP BY order_id
)

SELECT
    order_id,
    P_RAND_GEN(CONCAT(CAST(order_id AS STRING),'-order-base'), {{ n_customers }}) AS customer_id,
    order_size,
    total_goods_100,
    total_goods_vat_100,
    order_vat_rate_100,

    CASE
        WHEN MOD(order_id, 1000) < {{ online_share_1000 }} THEN 'online'
        ELSE 'store'
    END AS order_channel,

    CASE
        WHEN MOD(order_id, 1000) < {{ online_share_1000 }}
            THEN
                CASE
                    WHEN order_size > {{ parcel_max_items }} THEN 'truck'
                    ELSE 'parcel'
                END
        ELSE 'store_pickup'
    END AS delivery_type

FROM lines_agg
