{{ config(materialized='view') }}

with kpis_description_combined as (

    SELECT * from (VALUES 
      ('dbt1', 'Saldo pendent', 'Saldo pendent (Saldopendent_a + saldo_pendent_b). "Import de les factures amb deute (sumen els pendent de les factures amb deute) Import de les extra lines de les factures amb Descripcio = fracció. Aquest resultat és el que queda pendent dels fraccionaments"', '2022-10-06'::timestamptz::text),
      ('dbt2', 'Fraccionament: Import', 'Fraccionament: Import (Fraccionament: Import_a Fraccionament: Import_b)', '2022-10-06'::timestamptz::text),
      ('dbt3', 'Import factures en procediment de tall: Import', 'Import factures en procediment de tall: Import ((cob2 + cob3) - cob15 - cob4 - cob6 - cob8 - cob9 - cob13)', '2022-10-06'::timestamptz::text),
      ('dbt4', 'Import factures en procediment de tall: Número', 'Import factures en procediment de tall: Número ((cob1) - cob16 - cob5 - cob7 - cob10 - cob14)', '2022-10-06'::timestamptz::text),
      ('dbt10', 'Sol.licituts d''altes (A3 nous)', 'Casos A3 nous. Tos els casos A3 existents ahir - tots els casos existents abans d''ahir (con2 - con2 d''ahir). Segurament siguint tots A3 01 noves. Altes de subminitrament sol.licitades', '2022-10-06'::timestamptz::text),
      ('dbt11', 'Sol.licituds de contractació o canvi de comercialitzadora (Cs noves)', 'Casos CX nous (segurament 01 creats o 11 rebuts). Noves sol.licituts de contractació amb la cooperativa. (con5 - con5 d''ahir)', '2022-10-06'::timestamptz::text),
      ('dbt12', 'Sol.licituts de modificació (M1 nous)',	'Casos M1 nous. Tos els casos M1 existents ahir - tots els casos existents abans d''ahir. Segurament siguint tots M1 01 nous. Modificacions de contracte sol.licitades. (con7 - con7 d''ahir)', '2022-10-06'::timestamptz::text),
      ('dbt13', 'Sol.licituts de baixa (B1 nous)', 'Casos B1 nous. Tos els casos B1 existents ahir - tots els casos existents abans d''ahir. Segurament siguint tots B1 01 nous. Baixes de contracte sol.licitades. (con9 - con9 d''ahir)', '2022-10-06'::timestamptz::text),
      ('dbt14', 'Indic Endarrerida - *Percentatge Endarrerida vs total contractes', 'Utilitzant fac32 i fac33 per fer el percentatge', '2022-10-07'::timestamptz::text),
      ('dbt15', 'Casos de reclamació en marxa (CAC actius equip) pendent o obert', 'Suma de rec1 i rec2', '2022-10-14'::timestamptz::text)
      )
    AS t (code, name, description, create_date)
)
SELECT
    code, name, description, create_date,
    interval '1 days' as day_offset
FROM {{ source('erp_operational','erppeek_kpis_description') }}
UNION
SELECT
    code, name, description, create_date, interval '1 days' as day_offset
FROM kpis_description_combined
