{{ config(
    materialized = 'ephemeral',
    cluster_by=['order_id']
) }}

{% set pf_min_spend_100 = var('parcel_free_delivery_min_spend_100') %}
{% set tf_min_spend_100 = var('truck_free_delivery_min_spend_100') %}
{% set parcel_fee_100 = var('parcel_fee_100') %}
{% set truck_fee_100 = var('truck_fee_100') %}


SELECT
    order_id,
    order_channel,
    delivery_type,
    total_goods_100,
    CASE
        WHEN order_channel = 'store' THEN 0
        WHEN delivery_type = 'parcel'
            THEN
                CASE
                    WHEN total_goods_100 >= {{ pf_min_spend_100 }} THEN 0
                    ELSE {{ parcel_fee_100 }}
                END
        WHEN total_goods_100 >= {{ tf_min_spend_100 }} THEN 0
        ELSE {{ truck_fee_100 }}
    END AS picking_delivery_100
FROM {{ ref('int_orders_base') }}
