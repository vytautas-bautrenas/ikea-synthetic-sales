{{ config(
    materialized = 'table',
    cluster_by = ['country', 'grid_lat']
) }}

-- 1. District-level centroid coordinates from ONSPD
WITH district_coordinates AS (
    SELECT
        postcode_district,
        AVG(lat) AS latitude,
        AVG(long) AS longitude,
        MAX(lad25nm) AS local_authority,
        COALESCE(MAX(country), 'United Kingdom') AS country
    FROM {{ ref('dim_postcodes') }}
    GROUP BY postcode_district
),

-- 2. Aggregate orders by customer postcode district
customer_districts AS (
    SELECT
        customer_id,
        SPLIT(postcode, ' ')[OFFSET(0)] AS postcode_district
    FROM {{ ref('dim_customers') }}
),

district_orders AS (
    SELECT
        cd.postcode_district,
        COUNT(o.order_id) AS total_orders,
        SUM(o.total_sales_100) / 100.0 AS total_sales_gbp
    FROM {{ ref('fact_orders') }} AS o
    INNER JOIN customer_districts AS cd
        ON o.customer_id = cd.customer_id
    GROUP BY cd.postcode_district
),

-- 3. Snap districts directly to 10km grid cells (~0.09° lat x ~0.15° long)
district_grid AS (
    SELECT
        ROUND(FLOOR(dc.latitude / 0.09) * 0.09, 4) AS grid_lat,
        ROUND(FLOOR(dc.longitude / 0.15) * 0.15, 4) AS grid_long,
        MAX(dc.local_authority) AS local_authority,
        MAX(dc.country) AS country,
        SUM(do.total_orders) AS total_orders,
        SUM(do.total_sales_gbp) AS total_sales_gbp,
        ROUND(SAFE_DIVIDE(SUM(do.total_sales_gbp), SUM(do.total_orders)), 2) AS avg_order_value_gbp
    FROM district_orders AS do
    INNER JOIN district_coordinates AS dc
        ON do.postcode_district = dc.postcode_district
    GROUP BY grid_lat, grid_long
)

SELECT
    grid_lat,
    grid_long,
    ROUND(grid_lat + 0.09, 4) AS max_lat,
    ROUND(grid_long + 0.15, 4) AS max_long,
    ROUND(grid_lat + 0.045, 4) AS center_lat,
    ROUND(grid_long + 0.075, 4) AS center_long,
    country,
    local_authority,
    total_orders,
    total_sales_gbp,
    avg_order_value_gbp
FROM district_grid
