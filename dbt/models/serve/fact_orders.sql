{{ config(
    materialized = 'table',
    cluster_by=['order_id'],
    sql_header = p_rand_gen_header()
) }}


WITH order_vat AS (
    SELECT
        b.order_id,
        ROUND(
        SAFE_DIVIDE(
            (b.total_goods_100 + pd.picking_delivery_100) * b.order_vat_rate_100,
            100 + b.order_vat_rate_100
        ),
        0
    ) AS total_vat_100
    FROM {{ ref('int_orders_base') }} AS b
    LEFT JOIN {{ ref('int_orders_picking_delivery') }} AS pd
        ON b.order_id = pd.order_id
)

SELECT
    b.order_id,
    b.order_channel,
    b.delivery_type,
    b.customer_id,
    d.order_datetime,
    d.dispatch_datetime,
    d.delivery_datetime,
    d.order_status,
    p.payment_method,
    b.order_size,
    b.total_goods_100,
    pd.picking_delivery_100,
    b.order_vat_rate_100,
    b.total_goods_100 + pd.picking_delivery_100 AS total_sales_100,
    ov.total_vat_100,
    b.total_goods_100 + pd.picking_delivery_100 - ov.total_vat_100 AS total_sales_excl_vat_100

FROM
    {{ ref('int_orders_base') }} AS b
LEFT JOIN
    {{ ref('int_orders_datetimes') }} AS d
    ON b.order_id = d.order_id
LEFT JOIN
    {{ ref('int_orders_payments') }} AS p
    ON b.order_id = p.order_id
LEFT JOIN
    {{ ref('int_orders_picking_delivery') }} AS pd
    ON b.order_id = pd.order_id
LEFT JOIN
    order_vat AS ov
    ON b.order_id = ov.order_id
