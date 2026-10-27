{% set bs_explicit = var('basket_size')['explicit'] %}
{% set payment_weights_store = var('payment_method_weights')['store'] %}
{% set payment_weights_online = var('payment_method_weights')['online'] %}
{% set store_weight_total = payment_weights_store.values() | sum %}
{% set online_weight_total = payment_weights_online.values() | sum %}
{% set order_history_seconds = var('order_history_seconds') %}
{% set fault_tolerance_sigma = var('fault_tolerance_sigma') %}

WITH orders_stats AS (
    SELECT
        COUNT(*) AS total_orders,
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
    {% for size, p in (bs_explicit.items() | list)[:3] %}
    SELECT
        'basket_size_{{ size }}_share' AS metric_name,
        {{ p }} AS expected_value,
        SAFE_DIVIDE(bs_{{ size }}_count, total_orders) AS actual_value,
        SQRT(SAFE_DIVIDE({{ p }} * (1.0 - {{ p }}), total_orders)) AS std_error
    FROM orders_stats
    UNION ALL
    {% endfor %}

    {% for method, weight in (payment_weights_store.items() | list)[:3] %}
    {% set p = weight / store_weight_total %}
    SELECT
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
        'pay_online_{{ method }}_share',
        {{ p }},
        SAFE_DIVIDE(online_{{ method }}_count, online_orders),
        SQRT(SAFE_DIVIDE({{ p }} * (1.0 - {{ p }}), online_orders))
    FROM orders_stats
    UNION ALL
    {% endfor %}

    SELECT
        'order_history_seconds',
        {{ order_history_seconds }} - 1,
        order_history_window,
        SQRT(2.0) * SAFE_DIVIDE({{ order_history_seconds }}, total_orders)
    FROM orders_stats
)

SELECT
    metric_name,
    expected_value,
    actual_value,
    ABS(actual_value - expected_value) AS diff,
    SAFE_DIVIDE(ABS(actual_value - expected_value), std_error) AS sigma_departure,
    {{ fault_tolerance_sigma }} AS fault_tolerance_sigma
FROM metrics
WHERE SAFE_DIVIDE(ABS(actual_value - expected_value), std_error) > {{ fault_tolerance_sigma }}
