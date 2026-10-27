{{ config(
    materialized = 'ephemeral',
    cluster_by=['order_id'],
    sql_header = p_rand_gen_header()
) }}

{% set pg = var('p_granularity') %}

{% set online_weights = var('payment_method_weights')['online'] %}
{% set store_weights = var('payment_method_weights')['store'] %}
{% set online_weight_total = online_weights.values() | sum %}
{% set store_weight_total = store_weights.values() | sum %}


WITH order_history AS (
    SELECT
        order_id,
        order_size,
        total_goods_100,
        total_goods_vat_100,
        order_vat_rate_100,
        order_channel,
        delivery_type,
        P_RAND_GEN(CONCAT(CAST(order_id AS STRING), '-payment'), {{ pg }}) / {{ pg }} AS order_id_hash,
        {{ online_weight_total }} AS online_weight_total,
        {{ store_weight_total }} AS store_weight_total
    FROM
        {{ ref('int_orders_base') }}
),

payment_method_bands AS (

    SELECT *
    FROM UNNEST([
        {% set ns = namespace(cumulative=0) %}
        {% for method, weight in online_weights.items() %}
            STRUCT(
                'online' AS order_channel,
                '{{ method }}' AS payment_method,
                {{ ns.cumulative / online_weight_total }} AS lower_bound,
                {% if loop.last %}
                    1.0
                {% else %}
                    {{ (ns.cumulative + weight) / online_weight_total }}
                {% endif %} AS upper_bound
            )
            {% set ns.cumulative = ns.cumulative + weight %}
            {% if not loop.last %},{% endif %}
        {% endfor %},

        {% set ns = namespace(cumulative=0) %}
        {% for method, weight in store_weights.items() %}
            STRUCT(
                'store' AS order_channel,
                '{{ method }}' AS payment_method,
                {{ ns.cumulative / store_weight_total }} AS lower_bound,
                {% if loop.last %}
                    1.0
                {% else %}
                    {{ (ns.cumulative + weight) / store_weight_total }}
                {% endif %} AS upper_bound
            )
            {% set ns.cumulative = ns.cumulative + weight %}
            {% if not loop.last %},{% endif %}
        {% endfor %}
    ])
)

SELECT
    order_id,
    order_size,
    total_goods_100,
    total_goods_vat_100,
    order_vat_rate_100,
    oh.order_channel,
    delivery_type,
    payment_method
FROM
    order_history AS oh
INNER JOIN payment_method_bands AS pmb
    ON
        oh.order_channel = pmb.order_channel
        AND oh.order_id_hash >= pmb.lower_bound
        AND oh.order_id_hash < pmb.upper_bound
