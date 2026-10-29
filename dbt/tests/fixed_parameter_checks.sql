SELECT
    metric_name,
    expected_value,
    actual_value,
    variance_abs,
    max_allowed_variance,
    status
FROM {{ ref('inputs_vs_outputs_checks') }}
WHERE
    control_domain = 'Fixed Parameter Checks'
    AND status = 'FAIL'
