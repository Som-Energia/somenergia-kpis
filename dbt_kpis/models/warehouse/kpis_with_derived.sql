{{ config(materialized='view') }}

with kpis_wider_asc as (
  select * from {{ref('kpis_wider')}} order by create_date asc
)

SELECT
  *,
  cob2 + cob3 as dbt1,
  cob8 + cob9 as dbt2,
  (cob2 + cob3) - cob15 - cob4 - cob6 - cob8 - cob9 - cob13 as dbt3,
  (cob1) - cob16 - cob5 - cob7 - cob10 - cob14 as dbt4,
  (fac32::decimal / fac33)*100 as dbt14,
  con2 - lag(con2,1) over() as dbt10,
  con5 - lag(con5,1) over() as dbt11,
  con7 - lag(con7,1) over() as dbt12,
  con9 - lag(con9,1) over() as dbt13
FROM kpis_wider_asc

-- Saldo pendent (Saldopendent_a + saldo_pendent_b) as DBT1
-- Fraccionament: Import (Fraccionament: Import_a Fraccionament: Import_b) as DBT2
-- Import factures en procediment de tall: Import then DBT3
-- Import factures en procediment de tall: Número then DBT4