{{ config(
    materialized = 'ephemeral',
    sql_header = p_rand_gen_header()
) }}

{% set n_orders = var('n_orders') %}
{% set pg = var('p_granularity') %}
{% set chunk = var('gen_chunk_size') %}
{% set bs = var('basket_size') %}
{% set explicit = bs['explicit'] %}
{% set decay = bs['tail_decay'] %}
{% set max_size = bs['max_size'] %}
{% set tail_start = (explicit.keys() | max) + 1 %}
{% set tail_weight = 1.0 - (explicit.values() | sum) %}


WITH size_p AS (
    SELECT * FROM
        UNNEST([
            {% for size, p in explicit.items() %}
                STRUCT({{ size }} AS size, {{ p }} AS p){{ "," if not loop.last }}
            {% endfor %}
        ])
    UNION ALL
    SELECT
        s AS size,
        {{ tail_weight | round(12) }} * {{ decay }} * POW(1 - {{ decay }}, s - {{ tail_start }}) AS p
    FROM UNNEST(GENERATE_ARRAY({{ tail_start }}, {{ max_size }})) AS s
),

cdf_bounds AS (
    SELECT
        size,
        SUM(p) OVER (ORDER BY size) / SUM(p) OVER () AS cum,
        COALESCE(
            SUM(p) OVER (
                ORDER BY size
                ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
            ),
            0
        ) / SUM(p) OVER () AS prev
    FROM size_p
),

order_size_slots AS (
    SELECT
        size,
        slot
    FROM cdf_bounds,
        UNNEST(GENERATE_ARRAY(
            CAST(ROUND(prev * {{ pg }}, 0) AS INT64),
            CAST(ROUND(cum * {{ pg }}, 0) AS INT64) - 1
        )) AS slot
),

n AS (
    SELECT i FROM {{ ref('int_number_expansion') }}
    WHERE i < ({{ n_orders }} + {{ chunk }} - 1) / {{ chunk }}
),

orders AS (
    SELECT n.i * {{ chunk }} + a AS order_id
    FROM n
    CROSS JOIN UNNEST(GENERATE_ARRAY(1, {{ chunk }})) AS a
    WHERE n.i * {{ chunk }} + a <= {{ n_orders }}
),

slotted AS (
    SELECT
        order_id,
        P_RAND_GEN(CONCAT(CAST(order_id AS STRING), '-order'), {{ pg }}) AS slot
    FROM orders
)

SELECT
    s.order_id,
    b.size AS basket_size
FROM slotted AS s
INNER JOIN order_size_slots AS b ON s.slot = b.slot
