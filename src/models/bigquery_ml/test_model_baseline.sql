/* ==========================================================================
   ANALYSIS: COEFFICIENTS & STATISTICAL SIGNIFICANCE (BASELINE)
   Objective: Determine which variables are statistically relevant based on P-Values.
   Threshold: P-Value < 0.05 indicates statistical significance at 95% confidence.
   ========================================================================== */

SELECT
    'Model A (Linear)' AS model_name,
    processed_input AS feature_name,
    ROUND(weight, 2) AS coefficient, -- Weight represents direct monetary impact
    ROUND(standard_error, 2) AS standard_error,
    ROUND(p_value, 4) AS p_value,
    CASE
        WHEN p_value < 0.01 THEN '*** Highly Significant'
        WHEN p_value < 0.05 THEN '** Significant'
        WHEN p_value < 0.10 THEN '* Marginally Significant'
        ELSE 'Not Significant (Likely Noise)'
    END AS significance_level
FROM ML.ADVANCED_WEIGHTS(MODEL `real-estate-qro.queretaro_data_marts.model_baseline_linear`)

UNION ALL

SELECT
    'Model B (Log-Linear)' AS model_name,
    processed_input AS feature_name,
    ROUND(weight, 4) AS coefficient, -- Weight represents approximate % change (elasticity)
    ROUND(standard_error, 4) AS standard_error,
    ROUND(p_value, 4) AS p_value,
    CASE
        WHEN p_value < 0.01 THEN '*** Highly Significant'
        WHEN p_value < 0.05 THEN '** Significant'
        WHEN p_value < 0.10 THEN '* Marginally Significant'
        ELSE 'Not Significant (Likely Noise)'
    END AS significance_level
FROM ML.ADVANCED_WEIGHTS(MODEL `real-estate-qro.queretaro_data_marts.model_baseline_log_linear`)

ORDER BY model_name, feature_name;