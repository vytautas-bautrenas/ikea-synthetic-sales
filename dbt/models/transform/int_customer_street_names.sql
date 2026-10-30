{{ config(
    materialized = 'table',
    cluster_by = ['postcode_id'],
    sql_header = p_rand_gen_header()
) }}

{% set pg = var('p_granularity') %}

WITH postcodes AS (
    SELECT
        pcds,
        postcode_district,
        lad25cd,
        bua24cd,
        country,
        postcode_area,
        city_town,
        ROW_NUMBER() OVER (PARTITION BY postcode_district ORDER BY pcds) - 1 AS district_rank,
        P_RAND_GEN(pcds, {{ pg }}) AS fallback_slot
    FROM {{ ref('dim_postcodes') }}
),

street_names AS (
    SELECT
        postcode_district,
        road_name,
        ROW_NUMBER() OVER (PARTITION BY postcode_district ORDER BY RAND()) - 1 AS name_rank,
        COUNT(*) OVER (PARTITION BY postcode_district) AS names_available
    FROM {{ source('ingest', 'uk_street_names') }}
),

fallback_slots AS (
    SELECT
        road_name,
        slot
    FROM (
        SELECT
            road_name,
            ROW_NUMBER() OVER (ORDER BY RAND()) - 1 AS name_rank,
            COUNT(*) OVER () AS names_available
        FROM
            (
                SELECT DISTINCT road_name
                FROM {{ source('ingest', 'uk_street_names') }}
            )
    ),
        UNNEST(GENERATE_ARRAY(
            CAST(ROUND(name_rank * {{ pg }} / names_available, 0) AS INT64),
            CAST(ROUND((name_rank + 1) * {{ pg }} / names_available, 0) AS INT64) - 1
        )) AS slot
)

SELECT
    p.pcds AS postcode,
    p.postcode_district,
    p.city_town,
    p.postcode_area,
    p.country,
    ROW_NUMBER() OVER (ORDER BY p.pcds) - 1 AS postcode_id,
    COALESCE(sn.road_name, fb.road_name) AS street_name,
    IF(sn.road_name IS NULL, 'fallback', 'district') AS street_name_level
FROM postcodes AS p
LEFT JOIN street_names AS sn
    ON
        p.postcode_district = sn.postcode_district
        AND sn.name_rank = MOD(p.district_rank, sn.names_available)
LEFT JOIN fallback_slots AS fb
    ON
        sn.road_name IS NULL
        AND p.fallback_slot = fb.slot
