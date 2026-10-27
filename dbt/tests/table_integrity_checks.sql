WITH lines AS (
    SELECT
        COUNT(*) AS total_lines,
        SUM(line_total_100) AS total_goods_100
    FROM {{ ref('fact_order_lines') }}
),
orders AS (
    SELECT
        SUM(order_size) AS total_order_items,
        SUM(total_goods_100) AS total_goods_100
    FROM {{ ref('fact_orders') }}
)
SELECT
    l.total_goods_100 AS lines_total_goods_100,
    o.total_goods_100 AS orders_total_goods_100,
    l.total_lines,
    o.total_order_items
FROM lines l
CROSS JOIN orders o
WHERE l.total_goods_100 != o.total_goods_100
   OR l.total_lines != o.total_order_items
