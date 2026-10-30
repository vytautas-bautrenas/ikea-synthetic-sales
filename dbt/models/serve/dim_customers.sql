{{ config(
    materialized = 'table',
    cluster_by = ['customer_id', 'postcode'],
    sql_header = p_rand_gen_header()
) }}

{% set n_customers = var('n_customers') %}
{% set pg = var('p_granularity') %}
{% set house_number_max = var('house_number_max') %}
{% set flat_percent = (var('flat_share') * 100) | round(0) | int %}

WITH np AS (
    SELECT COUNT(*) AS n_postcodes
    FROM {{ ref('dim_postcodes') }}
),

drawn AS (
    SELECT
        num.i AS customer_id,
        P_RAND_GEN(CONCAT(CAST(num.i AS STRING), '-forename'), {{ pg }}) AS forename_slot,
        P_RAND_GEN(CONCAT(CAST(num.i AS STRING), '-surname'), {{ pg }}) AS surname_slot,
        P_RAND_GEN(CONCAT(CAST(num.i AS STRING), '-postcode'), np.n_postcodes) AS postcode_draw,
        P_RAND_GEN(CONCAT(CAST(num.i AS STRING), '-house'), {{ house_number_max }}) AS house_draw,
        P_RAND_GEN(CONCAT(CAST(num.i AS STRING), '-flat'), 100) AS flat_draw,
        P_RAND_GEN(CONCAT(CAST(num.i AS STRING), '-flat-label'), 40) AS flat_label_draw,
        P_RAND_GEN(CONCAT(CAST(num.i AS STRING), '-email'), 10000) AS email_draw
    FROM {{ ref('int_number_expansion') }} AS num
    CROSS JOIN np
    WHERE num.i < {{ n_customers }}
)

SELECT
    d.customer_id,
    f.forename AS first_name,
    s.surname AS last_name,

    CONCAT(
        IF(
            d.flat_draw < {{ flat_percent }},
            CONCAT(
                'Flat ',
                IF(
                    d.flat_label_draw < 20,
                    CAST(d.flat_label_draw + 1 AS STRING),
                    CHR(65 + d.flat_label_draw - 20)
                ),
                ', '
            ),
            ''
        ),
        CAST(d.house_draw + 1 AS STRING),
        ' ',
        sn.street_name
    ) AS address_line_1,

    sn.city_town,
    sn.postcode_area,
    sn.postcode,
    sn.country,
    CONCAT(LOWER(f.forename), '.', LOWER(s.surname), CAST(d.email_draw AS STRING), '@example.com') AS email
FROM drawn AS d
LEFT JOIN {{ ref('int_customer_forenames') }} AS f
    ON d.forename_slot = f.slot
LEFT JOIN {{ ref('int_customer_surnames') }} AS s
    ON d.surname_slot = s.slot
LEFT JOIN {{ ref('int_customer_street_names') }} AS sn
    ON d.postcode_draw = sn.postcode_id
