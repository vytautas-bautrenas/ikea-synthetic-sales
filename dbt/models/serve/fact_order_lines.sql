{{ config(
    materialized = 'view'
) }}

WITH unnested AS (
    SELECT
        o.order_id,
        ol.product_id,
        ol.unit_price_100,
        ol.unit_vat_100,
        ol_offset + 1 AS line_number
    FROM {{ ref('fact_orders') }} AS o,
        UNNEST(o.order_lines) AS ol WITH OFFSET AS ol_offset
)

SELECT
    order_id,
    line_number,
    product_id,
    unit_price_100 AS line_price_100,
    unit_price_100 AS line_total_100,
    unit_vat_100 AS line_vat_100,
    1 AS line_quantity,
    unit_price_100 - unit_vat_100 AS line_total_excl_vat_100
FROM unnested
