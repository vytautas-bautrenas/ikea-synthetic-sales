{{ config(
    materialized='table',
    cluster_by=['slot']
) }}

-- Surname dictionary with cumulative distribution bounds.
--
-- Same shape and same reasoning as int_names: the DfE school-census list of the
-- top 1,000 surnames with real counts, turned into (cdf_prev, cdf_cum] fractions
-- that dim_customer expands into slots for a weighted equi-join.
--
-- 1,000 forenames x 1,000 surnames is a one-million name-combination space,
-- which is far more repetition than reality: the top 100 UK surnames alone cover
-- roughly a fifth of the population.

{% set pg = var('p_granularity') %}

WITH surnames AS (
    SELECT
        -- 217 source entries are transcribed in full caps; normalise only those,
        -- so mixed-case names (O'Brien, McDonald) stay exactly as the DfE shipped them
        name_count,

        IF(
            surname = UPPER(surname),
            INITCAP(surname),
            surname
        ) AS surname,

        SUM(name_count) OVER (
            ORDER BY name_count DESC, surname ASC
        ) / SUM(name_count) OVER () AS cdf_cum,

        COALESCE(
            SUM(name_count) OVER (
                ORDER BY name_count DESC, surname ASC
                ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
            ),
            0
        ) / SUM(name_count) OVER () AS cdf_prev

    FROM {{ source('ingest', 'uk_census_surnames') }}
)

SELECT
    surname,
    slot
FROM surnames,
    UNNEST(GENERATE_ARRAY(
        CAST(ROUND(cdf_prev * {{ pg }}, 0) AS INT64),
        CAST(ROUND(cdf_cum * {{ pg }}, 0) AS INT64) - 1
    )) AS slot
