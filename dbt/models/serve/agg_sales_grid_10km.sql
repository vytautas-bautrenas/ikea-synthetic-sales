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

-- 2. Pre-aggregate orders by customer, then group by customer postcode district
customer_orders AS (
    SELECT
        customer_id,
        COUNT(*) AS total_orders,
        SUM(total_sales_100) AS total_sales_100
    FROM {{ ref('fact_orders') }}
    GROUP BY customer_id
),

customer_districts AS (
    SELECT
        customer_id,
        SPLIT(postcode, ' ')[OFFSET(0)] AS postcode_district
    FROM {{ ref('dim_customers') }}
),

district_orders AS (
    SELECT
        cd.postcode_district,
        SUM(co.total_orders) AS total_orders,
        SUM(co.total_sales_100) / 100.0 AS total_sales_gbp
    FROM customer_orders AS co
    INNER JOIN customer_districts AS cd
        ON co.customer_id = cd.customer_id
    GROUP BY cd.postcode_district
),

-- 3. Snap districts directly to 10km grid cells (~0.09° lat x ~0.15° long)
district_grid AS (
    SELECT
        ROUND(FLOOR(dc.latitude / 0.09) * 0.09, 4) AS grid_lat,
        ROUND(FLOOR(dc.longitude / 0.15) * 0.15, 4) AS grid_long,
        MAX(dc.local_authority) AS local_authority,
        MAX(dc.country) AS country,
        SUM(dord.total_orders) AS total_orders,
        SUM(dord.total_sales_gbp) AS total_sales_gbp,
        ROUND(SAFE_DIVIDE(SUM(dord.total_sales_gbp), SUM(dord.total_orders)), 2) AS avg_order_value_gbp
    FROM district_orders AS dord
    INNER JOIN district_coordinates AS dc
        ON dord.postcode_district = dc.postcode_district
    GROUP BY grid_lat, grid_long
)

SELECT
    grid_lat,
    grid_long,
    country,
    local_authority,
    total_orders,
    total_sales_gbp,
    avg_order_value_gbp,
    ROUND(grid_lat + 0.09, 4) AS max_lat,
    ROUND(grid_long + 0.15, 4) AS max_long,
    ROUND(grid_lat + 0.045, 4) AS center_lat,
    ROUND(grid_long + 0.075, 4) AS center_long
FROM district_grid
