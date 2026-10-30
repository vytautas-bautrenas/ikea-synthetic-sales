{{ config(
    materialized = 'table',
    partition_by = {
        "field": "p",
        "data_type": "int64",
        "range": {"start": 0, "end": 2000, "interval": 1}
    }
) }}

-- Persistent 0-based integer sequence; the driver for all row generation.
-- UNNEST(GENERATE_ARRAY(<literal>)) is planned with parallelInputs = 1, so one
-- slot emits every row. A stage that READS A TABLE gets one parallel input per
-- storage chunk. Partitioning across 2,000 modulo buckets forces 2,000 storage
-- partitions so downstream generation stages fan out across 2,000 workers.
-- Must be 0-based: dim_numbers i = 0..n-1 so order_id starts at 1.

{% set n_numbers = var('n_numbers') %}

SELECT
    a * 100000 + b - 1 AS i,
    MOD(a * 100000 + b - 1, 2000) AS p
FROM UNNEST(GENERATE_ARRAY(0, {{ n_numbers // 100000 - 1 }})) AS a
CROSS JOIN UNNEST(GENERATE_ARRAY(1, 100000)) AS b
