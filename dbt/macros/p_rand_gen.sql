{% macro p_rand_gen_header() %}
  CREATE TEMP FUNCTION P_RAND_GEN(input STRING, n_digits INT64)
  RETURNS INT64 AS (ABS(MOD(FARM_FINGERPRINT(input), n_digits)));
{% endmacro %}
