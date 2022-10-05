{{ config(materialized='view') }}

with new_kpis as (
    select
        created_date,
        value,
        LAG(value,1) OVER (
            PARTITION BY code
            ORDER BY created_date
            ) previous_value,
        case
            WHEN code 'CON2' then 'CON10' --'Sol.licituts d'altes (A3 nous)'
            WHEN code 'CON5' then 'CON11'	--'Sol.licituds de contractació o canvi de comercialitzadora (Cs noves)'
            WHEN code 'CON7' then 'CON12'	--'Sol.licituts de modificació (M1 nous)'
            WHEN code 'CON9' then 'CON13' --'Sol.licituts de baixa (B1 nous)'
                ELSE NULL
            END as new_code,
    from {{source('datasources', 'pilotatge_int_kpis')}}
    order by code, kpi_date
)
select
  kpi_date as kpi_date,
  new_code as code,
  case
    when new_code = 'CON10' then "Casos A3 nous. Tos els casos A3 existents ahir - tots els casos existents abans d'ahir. Segurament siguint tots A3 01 noves. Altes de subminitrament sol.licitades"
    when new_code = 'CON11' then"Casos CX nous (segurament 01 creats o 11 rebuts). Noves sol.licituts de contractació amb la cooperativa."
    when new_code = 'CON12' then"Casos M1 nous. Tos els casos M1 existents ahir - tots els casos existents abans d'ahir. Segurament siguint tots M1 01 nous. Modificacions de contracte sol.licitades"
    when new_code = 'CON13' then "Casos B1 nous. Tos els casos B1 existents ahir - tots els casos existents abans d'ahir. Segurament siguint tots B1 01 nous. Baixes de contracte sol.licitades"
    else NULL
    end as description,
  (value-previous_value) as value
from new_kpis
where new_code is not null