{{ config(materialized='view') }}

SELECT
  *,
  cob2 + cob3 as dbt1,
  cob8 + cob9 as dbt2,
  (cob2 + cob3) - cob15 - cob4 - cob6 - cob8 - cob9 - cob13 as dbt3,
  (cob1) - cob16 - cob5 - cob7 - cob10 - cob14 as dbt4
FROM {{ref('kpis_wider')}}

-- Saldo pendent (Saldopendent_a + saldo_pendent_b) as DBT1
-- Fraccionament: Import (Fraccionament: Import_a Fraccionament: Import_b) as DBT2
-- Import factures en procediment de tall: Import then DBT3
-- Import factures en procediment de tall: Número then DBT4