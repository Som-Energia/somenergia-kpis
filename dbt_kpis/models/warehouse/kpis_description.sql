{{ config(materialized='view') }}

with kpis_description_combined as (

    SELECT * from (VALUES 
      ('dbt1', 'Saldo pendent', 'Saldo pendent (Saldopendent_a + saldo_pendent_b). "Import de les factures amb deute (sumen els pendent de les factures amb deute) Import de les extra lines de les factures amb Descripcio = fracció. Aquest resultat és el que queda pendent dels fraccionaments"', '2022-10-06'::timestamptz::text),
      ('dbt2', 'Fraccionament: Import', 'Fraccionament: Import (Fraccionament: Import_a Fraccionament: Import_b)', '2022-10-06'::timestamptz::text),
      ('dbt3', 'Import factures en procediment de tall: Import', 'Import factures en procediment de tall: Import ((cob2 + cob3) - cob15 - cob4 - cob6 - cob8 - cob9 - cob13)', '2022-10-06'::timestamptz::text),
      ('dbt4', 'Import factures en procediment de tall: Número', 'Import factures en procediment de tall: Número ((cob1) - cob16 - cob5 - cob7 - cob10 - cob14)', '2022-10-06'::timestamptz::text)
      )
    AS t (code, name, description, create_date)
)
SELECT
    code, name, description, create_date,
    case
            when filter ilike '%__7_days_ago__%' then interval '7 days'
            when filter ilike '%__3_days_ago__%' then interval '3 days'
            when filter ilike '%__yesterday__%' then interval '1 days'
            else interval '0 days'
    end as day_offset
FROM {{ source('erp_operational','erppeek_kpis_description') }}
UNION
SELECT
    code, name, description, create_date, NULL as day_offset
FROM kpis_description_combined
