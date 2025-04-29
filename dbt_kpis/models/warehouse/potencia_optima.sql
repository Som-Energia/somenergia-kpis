{{ config (
    materialized = 'view'
) }}

WITH calcul_potencia_optima_raw AS (

    SELECT
        contract_id,
        period_id,
        power_kw,
        max_power_kw,
        ROUND(
            max_power_kw,
            1
        ) AS optimal_power_kw,
        28.7 AS contracted_power_price_eur_kw_year
    FROM
        {{ ref ('calcul_potencia_optima_raw') }}
)
SELECT
    *,
    (power_kw - optimal_power_kw) * contracted_power_price_eur_kw_year AS expected_bill_savings_eur_year
FROM
    calcul_potencia_optima_raw
