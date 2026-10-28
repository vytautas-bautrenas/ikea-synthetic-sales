{{ config(
    materialized = 'table',
    cluster_by=['order_id'],
    sql_header = p_rand_gen_header()
) }}


{% set pg = var('p_granularity') %}
{% set online_share_1000 = var('online_share') * 1000 %}
{% set parcel_max_items = var('parcel_max_items') %}
{% set n_customers = var('n_customers') %}
{% set oh_seconds = var('order_history_seconds') %}

{% set pf_min_spend_100 = var('parcel_free_delivery_min_spend_100') %}
{% set tf_min_spend_100 = var('truck_free_delivery_min_spend_100') %}
{% set parcel_fee_100 = var('parcel_fee_100') %}
{% set truck_fee_100 = var('truck_fee_100') %}

{% set order_size_1 = var('dispatch_order_size_1') %}
{% set order_size_2 = var('dispatch_order_size_2') %}
{% set order_size_3 = var('dispatch_order_size_3') %}

{% set dispatch_hours_1 = var('dispatch_hours_order_size_1') %}
{% set dispatch_hours_2 = var('dispatch_hours_order_size_2') %}  
{% set dispatch_hours_3 = var('dispatch_hours_order_size_3') %}
{% set dispatch_hours_max = var('dispatch_hours_order_size_max') %}

{% set delivery_hours_1 = var('delivery_hours_order_size_1') %}
{% set delivery_hours_2 = var('delivery_hours_order_size_2') %}
{% set delivery_hours_3 = var('delivery_hours_order_size_3') %}
{% set delivery_hours_max = var('delivery_hours_order_size_max') %}

{% set online_weights = var('payment_method_weights')['online'] %}
{% set store_weights = var('payment_method_weights')['store'] %}
{% set online_weight_total = online_weights.values() | sum %}
{% set store_weight_total = store_weights.values() | sum %}


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
),

calculated_orders AS (
    SELECT
        order_id,
        order_size,
        total_goods_100,
        order_vat_rate_100,
        P_RAND_GEN(CONCAT(CAST(order_id AS STRING), '-order-base'), {{ n_customers }}) AS customer_id,

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
        END AS delivery_type,

        CASE
            WHEN MOD(order_id, 1000) >= {{ online_share_1000 }} THEN 0
            WHEN order_size <= {{ parcel_max_items }}
                THEN
                    CASE
                        WHEN total_goods_100 >= {{ pf_min_spend_100 }} THEN 0
                        ELSE {{ parcel_fee_100 }}
                    END
            WHEN total_goods_100 >= {{ tf_min_spend_100 }} THEN 0
            ELSE {{ truck_fee_100 }}
        END AS picking_delivery_100,

        TIMESTAMP_SUB(
            CURRENT_TIMESTAMP(),
            INTERVAL P_RAND_GEN(CONCAT(
                CAST(order_id AS STRING),
                '-datetimes'
            ), {{ oh_seconds }}) SECOND
        ) AS order_datetime,

        P_RAND_GEN(CONCAT(CAST(order_id AS STRING), '-payment'), {{ pg }}) / {{ pg }} AS order_id_hash
    FROM lines_agg
),

orders_timeline AS (
    SELECT
        order_id,
        order_channel,
        delivery_type,
        customer_id,
        order_datetime,
        order_size,
        total_goods_100,
        picking_delivery_100,
        order_vat_rate_100,

        CASE
            WHEN order_channel = 'store' THEN order_datetime
            WHEN order_size <= {{ order_size_1 }} THEN TIMESTAMP_ADD(order_datetime, INTERVAL {{ dispatch_hours_1 }} HOUR)
            WHEN order_size <= {{ order_size_2 }} THEN TIMESTAMP_ADD(order_datetime, INTERVAL {{ dispatch_hours_2 }} HOUR)
            WHEN order_size <= {{ order_size_3 }} THEN TIMESTAMP_ADD(order_datetime, INTERVAL {{ dispatch_hours_3 }} HOUR)
            ELSE TIMESTAMP_ADD(order_datetime, INTERVAL {{ dispatch_hours_max }} HOUR)
        END AS dispatch_datetime_candidate,

        CASE
            WHEN order_channel = 'store' THEN order_datetime
            WHEN order_size <= {{ order_size_1 }} THEN TIMESTAMP_ADD(order_datetime, INTERVAL {{ dispatch_hours_1 }} + {{ delivery_hours_1 }} HOUR)
            WHEN order_size <= {{ order_size_2 }} THEN TIMESTAMP_ADD(order_datetime, INTERVAL {{ dispatch_hours_2 }} + {{ delivery_hours_2 }} HOUR)
            WHEN order_size <= {{ order_size_3 }} THEN TIMESTAMP_ADD(order_datetime, INTERVAL {{ dispatch_hours_3 }} + {{ delivery_hours_3 }} HOUR)
            ELSE TIMESTAMP_ADD(order_datetime, INTERVAL {{ dispatch_hours_max }} + {{ delivery_hours_max }} HOUR)
        END AS delivery_datetime_candidate,

        CASE
            WHEN order_channel = 'online' THEN
                CASE
                    {% set ns = namespace(cumulative=0) %}
                    {% for method, weight in online_weights.items() %}
                        {% if loop.last %}
                        ELSE '{{ method }}'
                        {% else %}
                        WHEN order_id_hash < {{ (ns.cumulative + weight) / online_weight_total }} THEN '{{ method }}'
                        {% endif %}
                        {% set ns.cumulative = ns.cumulative + weight %}
                    {% endfor %}
                END
            ELSE
                CASE
                    {% set ns = namespace(cumulative=0) %}
                    {% for method, weight in store_weights.items() %}
                        {% if loop.last %}
                        ELSE '{{ method }}'
                        {% else %}
                        WHEN order_id_hash < {{ (ns.cumulative + weight) / store_weight_total }} THEN '{{ method }}'
                        {% endif %}
                        {% set ns.cumulative = ns.cumulative + weight %}
                    {% endfor %}
                END
        END AS payment_method,

        total_goods_100 + picking_delivery_100 AS total_sales_100,

        ROUND(
            SAFE_DIVIDE(
                (total_goods_100 + picking_delivery_100) * order_vat_rate_100,
                100 + order_vat_rate_100
            ),
            0
        ) AS total_vat_100

    FROM calculated_orders
)

SELECT
    order_id,
    order_channel,
    delivery_type,
    customer_id,
    order_datetime,
    CASE
        WHEN dispatch_datetime_candidate > CURRENT_TIMESTAMP() THEN NULL
        ELSE dispatch_datetime_candidate
    END AS dispatch_datetime,
    CASE
        WHEN delivery_datetime_candidate > CURRENT_TIMESTAMP() THEN NULL
        ELSE delivery_datetime_candidate
    END AS delivery_datetime,
    CASE
        WHEN order_channel = 'store' THEN 'order_collected'
        WHEN dispatch_datetime_candidate > CURRENT_TIMESTAMP() THEN 'order_placed'
        WHEN delivery_datetime_candidate > CURRENT_TIMESTAMP() THEN 'dispatched'
        ELSE 'delivered'
    END AS order_status,
    payment_method,
    order_size,
    total_goods_100,
    picking_delivery_100,
    order_vat_rate_100,
    total_sales_100,
    total_vat_100,
    total_sales_100 - total_vat_100 AS total_sales_excl_vat_100
FROM orders_timeline
