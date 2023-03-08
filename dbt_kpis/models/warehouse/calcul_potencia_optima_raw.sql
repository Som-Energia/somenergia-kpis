{{ config(
    materialized = 'view'
) }}

SELECT
    id as contract_id,
    periode_id as period_id,
    potencia::decimal as power_kw,
    maximetre::decimal as max_power_kw
FROM
    {{ source(
        'erp_derived',
        'calcul_potencia_optima'
    ) }}
