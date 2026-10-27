{{ config(materialized='table') }}

-- Persistent 0-based integer sequence; the driver for all row generation.
-- UNNEST(GENERATE_ARRAY(<literal>)) is planned with parallelInputs = 1, so one
-- slot emits every row. A stage that READS A TABLE gets one parallel input per
-- storage chunk. Measured on a 2.3bn-row basket build:
--   10m-row driver  (80 MB)  -> parallelInputs = 16  -> 84 s
--   100m-row driver (800 MB) -> parallelInputs = 200 -> 17 s
-- Must be 0-based: dim_numbers i = 0..n-1 so order_id starts at 1.

{% set n_numbers = var('n_numbers') %}

SELECT a * 100000 + b - 1 AS i
FROM UNNEST(GENERATE_ARRAY(0, {{ n_numbers // 100000 - 1 }})) AS a
CROSS JOIN UNNEST(GENERATE_ARRAY(1, 100000)) AS b
