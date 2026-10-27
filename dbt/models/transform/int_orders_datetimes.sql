{{ config(
    materialized = 'ephemeral',
    cluster_by=['order_id'],
    sql_header = p_rand_gen_header()
) }}

{% set oh_seconds = var('order_history_seconds') %}

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


WITH order_history AS (
    SELECT
        order_id,
        order_size,
        order_channel,
        -- Subtract a random offset (0 to 220,903,200 seconds) from CURRENT_TIMESTAMP()
        TIMESTAMP_SUB(
            CURRENT_TIMESTAMP(),
            INTERVAL P_RAND_GEN(CONCAT(
                CAST(order_id AS STRING),
                '-datetimes'
            ), {{ oh_seconds }}) SECOND
        ) AS order_datetime

    FROM
        {{ ref('int_orders_base') }}
),

order_datetime_history AS (
    SELECT
        order_id,
        order_size,
        order_channel,
        order_datetime,
        -- Calculate dispatch and delivery datetimes based on order size. In-store orders default to instant delivery
        -- (i.e. the assumption is that the customer picks up the order immediately).
        CASE
            WHEN order_channel = 'store' THEN order_datetime
            WHEN
                order_size <= {{ order_size_1 }}
                THEN TIMESTAMP_ADD(order_datetime, INTERVAL {{ dispatch_hours_1 }} HOUR)
            WHEN
                order_size <= {{ order_size_2 }}
                THEN TIMESTAMP_ADD(order_datetime, INTERVAL {{ dispatch_hours_2 }} HOUR)
            WHEN
                order_size <= {{ order_size_3 }}
                THEN TIMESTAMP_ADD(order_datetime, INTERVAL {{ dispatch_hours_3 }} HOUR)
            ELSE TIMESTAMP_ADD(order_datetime, INTERVAL {{ dispatch_hours_max }} HOUR)
        END AS dispatch_datetime_candidate,
        CASE
            WHEN order_channel = 'store' THEN order_datetime
            WHEN
                order_size <= {{ order_size_1 }}
                THEN
                    TIMESTAMP_ADD(
                        order_datetime,
                        INTERVAL {{ dispatch_hours_1 }} + {{ delivery_hours_1 }} HOUR
                    )
            WHEN
                order_size <= {{ order_size_2 }}
                THEN
                    TIMESTAMP_ADD(
                        order_datetime,
                        INTERVAL {{ dispatch_hours_2 }} + {{ delivery_hours_2 }} HOUR
                    )
            WHEN
                order_size <= {{ order_size_3 }}
                THEN
                    TIMESTAMP_ADD(
                        order_datetime,
                        INTERVAL {{ dispatch_hours_3 }} + {{ delivery_hours_3 }} HOUR
                    )
            ELSE
                TIMESTAMP_ADD(
                    order_datetime,
                    INTERVAL {{ dispatch_hours_max }} + {{ delivery_hours_max }} HOUR
                )
        END AS delivery_datetime_candidate
    FROM
        order_history
)

SELECT
    order_id,
    order_size,
    order_channel,
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
    END AS order_status
FROM
    order_datetime_history
