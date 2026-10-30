{{ config(
    materialized='table',
    cluster_by=['slot']
) }}


{% set pg = var('p_granularity') %}

WITH forenames AS (
    SELECT
        name_count,
        IF(forename = UPPER(forename), INITCAP(forename), forename) AS forename,
        SUM(name_count) OVER (ORDER BY name_count DESC, forename ASC) / SUM(name_count) OVER () AS cdf_cum,

        COALESCE(
            SUM(name_count) OVER (
                ORDER BY name_count DESC, forename ASC
                ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
            ),
            0
        ) / SUM(name_count) OVER () AS cdf_prev

    FROM {{ source('ingest', 'uk_census_forenames') }}
)

SELECT
    forename,
    slot
FROM forenames,
    UNNEST(GENERATE_ARRAY(
        CAST(ROUND(cdf_prev * {{ pg }}, 0) AS INT64),
        CAST(ROUND(cdf_cum * {{ pg }}, 0) AS INT64) - 1
    )) AS slot
