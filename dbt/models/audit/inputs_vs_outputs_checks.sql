{{ config(
    materialized = 'table'
) }}

{% set expected_n_orders = var('n_orders') %}
{% set expected_n_customers = var('n_customers') %}
{% set bs_explicit = var('basket_size')['explicit'] %}
{% set payment_weights_store = var('payment_method_weights')['store'] %}
{% set payment_weights_online = var('payment_method_weights')['online'] %}
{% set store_weight_total = payment_weights_store.values() | sum %}
{% set online_weight_total = payment_weights_online.values() | sum %}
{% set fault_tolerance_sigma = var('fault_tolerance_sigma') %}
{% set order_history_seconds = var('order_history_seconds') %}

WITH lines_stats AS (
    SELECT
        COUNT(*) AS total_lines,
        SUM(line_total_100) AS lines_total_100
    FROM {{ ref('fact_order_lines') }}
),

customers_stats AS (
    SELECT COUNT(*) AS total_customers
    FROM {{ ref('dim_customers') }}
),

orders_stats AS (
    SELECT
        COUNT(*) AS total_orders,
        SUM(order_size) AS total_order_items,
        SUM(total_goods_100) AS total_goods_100,
        COUNTIF(order_channel = 'store') AS store_orders,
        COUNTIF(order_channel = 'online') AS online_orders,
        TIMESTAMP_DIFF(MAX(order_datetime), MIN(order_datetime), SECOND) AS order_history_window,
        {% for size in (bs_explicit.keys() | list)[:3] %}
            COUNTIF(order_size = {{ size }}) AS bs_{{ size }}_count,
        {% endfor %}
        {% for method in (payment_weights_store.keys() | list)[:3] %}
            COUNTIF(order_channel = 'store' AND payment_method = '{{ method }}') AS store_{{ method }}_count,
        {% endfor %}
        {% for method in (payment_weights_online.keys() | list)[:3] %}
            COUNTIF(order_channel = 'online' AND payment_method = '{{ method }}') AS online_{{ method }}_count{{ "," if not loop.last }}
        {% endfor %}
    FROM {{ ref('fact_orders') }}
),

metrics AS (
    -- 1. Table Integrity Checks (exact match, std_error = 0)
    SELECT
        'Table Integrity Checks' AS control_domain,
        'lines_total_equals_orders_total_goods' AS metric_name,
        l.lines_total_100 AS expected_value,
        o.total_goods_100 AS actual_value,
        0.0 AS std_error
    FROM lines_stats AS l CROSS JOIN orders_stats AS o

    UNION ALL

    SELECT
        'Table Integrity Checks',
        'lines_count_equals_orders_items_sum',
        l.total_lines,
        o.total_order_items,
        0.0
    FROM lines_stats AS l CROSS JOIN orders_stats AS o

    UNION ALL

    -- 2. Fixed Parameter Checks (exact match, std_error = 0)
    SELECT
        'Fixed Parameter Checks',
        'n_orders_count',
        {{ expected_n_orders }},
        total_orders,
        0.0
    FROM orders_stats

    UNION ALL

    SELECT
        'Fixed Parameter Checks',
        'n_customers_count',
        {{ expected_n_customers }},
        total_customers,
        0.0
    FROM customers_stats

    UNION ALL

    -- 3. Variable Parameter Checks (statistical tolerance via std_error)
    {% for size, p in (bs_explicit.items() | list)[:3] %}
    SELECT
        'Variable Parameter Checks',
        'basket_size_{{ size }}_share',
        {{ p }},
        SAFE_DIVIDE(bs_{{ size }}_count, total_orders),
        SQRT(SAFE_DIVIDE({{ p }} * (1.0 - {{ p }}), total_orders))
    FROM orders_stats
    UNION ALL
    {% endfor %}

    {% for method, weight in (payment_weights_store.items() | list)[:3] %}
    {% set p = weight / store_weight_total %}
    SELECT
        'Variable Parameter Checks',
        'pay_store_{{ method }}_share',
        {{ p }},
        SAFE_DIVIDE(store_{{ method }}_count, store_orders),
        SQRT(SAFE_DIVIDE({{ p }} * (1.0 - {{ p }}), store_orders))
    FROM orders_stats
    UNION ALL
    {% endfor %}

    {% for method, weight in (payment_weights_online.items() | list)[:3] %}
    {% set p = weight / online_weight_total %}
    SELECT
        'Variable Parameter Checks',
        'pay_online_{{ method }}_share',
        {{ p }},
        SAFE_DIVIDE(online_{{ method }}_count, online_orders),
        SQRT(SAFE_DIVIDE({{ p }} * (1.0 - {{ p }}), online_orders))
    FROM orders_stats
    UNION ALL
    {% endfor %}

    SELECT
        'Variable Parameter Checks',
        'order_history_seconds',
        {{ order_history_seconds }} - 1,
        order_history_window,
        SQRT(2.0) * SAFE_DIVIDE({{ order_history_seconds }}, total_orders)
    FROM orders_stats
)

SELECT
    control_domain,
    metric_name,
    expected_value,
    actual_value,
    ABS(actual_value - expected_value) AS variance_abs,
    {{ fault_tolerance_sigma }} * std_error AS max_allowed_variance,
    IF(ABS(actual_value - expected_value) <= {{ fault_tolerance_sigma }} * std_error, 'PASS', 'FAIL') AS status
FROM metrics
ORDER BY control_domain, metric_name
