{{ config(materialized='table') }}

SELECT
    product_id,
    product_name AS product_series,
    main_category,
    sub_category,
    product_rating,
    product_rating_count,
    online_sellable,
    url,
    price_100 AS unit_price_100,
    CAST(0.20 AS NUMERIC) AS vat_rate,
    currency,
    CONCAT(product_name, ' ', product_type) AS product_name
FROM {{ source('ingest', 'ikea_product_catalog') }}
WHERE
    sale_tag IS DISTINCT FROM 'DISCONTINUED_PRODUCT'
    AND country = 'UK'
