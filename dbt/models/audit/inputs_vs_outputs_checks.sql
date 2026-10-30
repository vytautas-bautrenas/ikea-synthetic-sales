{{ config(
    materialized = 'table'
) }}

-- depends_on: {{ ref('fact_orders') }}
-- depends_on: {{ ref('dim_customers') }}

{% set expected_n_orders = var('n_orders') %}
{% set expected_n_customers = var('n_customers') %}
{% set audit_sample_size = var('audit_sample_size', 0.10) %}
{% set bs_explicit = var('basket_size')['explicit'] %}
{% set payment_weights_store = var('payment_method_weights')['store'] %}
{% set payment_weights_online = var('payment_method_weights')['online'] %}
{% set store_weight_total = payment_weights_store.values() | sum %}
{% set online_weight_total = payment_weights_online.values() | sum %}
{% set fault_tolerance_sigma = var('fault_tolerance_sigma') %}
{% set order_history_seconds = var('order_history_seconds') %}

WITH table_counts AS (
    SELECT
        MAX(IF(table_id = 'fact_orders', row_count, NULL)) AS total_orders,
        MAX(IF(table_id = 'dim_customers', row_count, NULL)) AS total_customers
    FROM `{{ target.project }}.serve.__TABLES__`
),

orders_sample AS (
    SELECT
        COUNT(*) AS sample_orders,
        SUM(order_size) AS sample_order_items,
        SUM(total_goods_100) AS sample_total_goods_100,
        SUM(ARRAY_LENGTH(order_lines)) AS sample_total_lines,
        SUM((
            SELECT SUM(unit_price_100)
            FROM UNNEST(order_lines)
        )) AS sample_lines_total_100,
        COUNTIF(order_channel = 'store') AS sample_store_orders,
        COUNTIF(order_channel = 'online') AS sample_online_orders,
        TIMESTAMP_DIFF(MAX(order_datetime), MIN(order_datetime), SECOND) AS order_history_window,
        {% for size in (bs_explicit.keys() | list)[:3] %}
            COUNTIF(order_size = {{ size }}) AS bs_{{ size }}_count,
        {% endfor %}
        {% for method in (payment_weights_store.keys() | list)[:3] %}
            COUNTIF(order_channel = 'store' AND payment_method = '{{ method }}') AS store_{{ method }}_count,
        {% endfor %}
        {% for method in (payment_weights_online.keys() | list)[:3] %}
            COUNTIF(order_channel = 'online' AND payment_method = '{{ method }}')
                AS online_{{ method }}_count{% if not loop.last %},{% endif %}
        {% endfor %}
    FROM
        {{ ref('fact_orders') }}
        TABLESAMPLE SYSTEM ({{ (audit_sample_size * 100) | int }} PERCENT)
)

SELECT
    m.control_domain,
    m.metric_name,
    m.expected_value,
    m.actual_value,
    ABS(m.actual_value - m.expected_value) AS variance_abs,
    {{ fault_tolerance_sigma }} * m.std_error AS max_allowed_variance,
    IF(ABS(m.actual_value - m.expected_value) <= {{ fault_tolerance_sigma }} * m.std_error, 'PASS', 'FAIL') AS status
FROM table_counts AS tc
CROSS JOIN orders_sample AS os
CROSS JOIN
    -- Using UNNEST([STRUCT()]) to enable BigQuery to use TABLESAMPLE for a random slice
    -- of the full fact_orders table for audit checks.
    UNNEST([
        -- 1. Table Integrity Checks (exact match on sampled orders, std_error = 0)
        STRUCT(
            'Table Integrity Checks' AS control_domain,
            'lines_total_equals_orders_total_goods' AS metric_name,
            os.sample_lines_total_100 AS expected_value,
            os.sample_total_goods_100 AS actual_value,
            0.0 AS std_error
        ),
        STRUCT(
            'Table Integrity Checks',
            'lines_count_equals_orders_items_sum',
            os.sample_total_lines,
            os.sample_order_items,
            0.0
        ),

        -- 2. Fixed Parameter Checks (exact metadata row counts, std_error = 0)
        STRUCT('Fixed Parameter Checks', 'n_orders_count', {{ expected_n_orders }}, tc.total_orders, 0.0),
        STRUCT('Fixed Parameter Checks', 'n_customers_count', {{ expected_n_customers }}, tc.total_customers, 0.0),

        -- 3. Variable Parameter Checks (statistical tolerance on sample)
        {% for size, p in (bs_explicit.items() | list)[:3] %}
            STRUCT(
                'Variable Parameter Checks',
                'basket_size_{{ size }}_share',
                {{ p }},
                SAFE_DIVIDE(os.bs_{{ size }}_count, os.sample_orders),
                SQRT(SAFE_DIVIDE({{ p }} * (1.0 - {{ p }}), os.sample_orders))
            ),
        {% endfor %}

        {% for method, weight in (payment_weights_store.items() | list)[:3] %}
            {% set p = weight / store_weight_total %}
            STRUCT(
                'Variable Parameter Checks',
                'pay_store_{{ method }}_share',
                {{ p }},
                SAFE_DIVIDE(os.store_{{ method }}_count, os.sample_store_orders),
                SQRT(SAFE_DIVIDE({{ p }} * (1.0 - {{ p }}), os.sample_store_orders))
            ),
        {% endfor %}

        {% for method, weight in (payment_weights_online.items() | list)[:3] %}
            {% set p = weight / online_weight_total %}
            STRUCT(
                'Variable Parameter Checks',
                'pay_online_{{ method }}_share',
                {{ p }},
                SAFE_DIVIDE(os.online_{{ method }}_count, os.sample_online_orders),
                SQRT(SAFE_DIVIDE({{ p }} * (1.0 - {{ p }}), os.sample_online_orders))
            ),
        {% endfor %}

        STRUCT(
            'Variable Parameter Checks',
            'order_history_seconds',
            {{ order_history_seconds }} - 1,
            os.order_history_window,
            SQRT(2.0) * SAFE_DIVIDE({{ order_history_seconds }}, os.sample_orders)
        )
    ]) AS m
ORDER BY m.control_domain, m.metric_name
