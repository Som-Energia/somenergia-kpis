{{ config(materialized='view') }}

-- TODO table the combination
with combinable_kpis as (
    select
    create_date,
    value,
    case
      when name like 'COB1COB2' or name like 'COB1' then +1
      else -1
    end as multiplier,
    case
      when code in (
        'COB2COB3', -- 'Saldo pendent', -- positive
        'COB15',    --'Factures impagades de contractes de baixa: Import',
        'COB4',     --'R1: Import',
        'COB6',     --'Pobresa: Import',
        'COB8COB9', --'Fraccionament: Import',
        'COB13'     --'Monitori: Import'
      ) then 'Import factures en procediment de tall: Import'
      when code in (
        'COB1',  -- 'Número de factures pendents' -- positive
        'COB16', -- 'Factures impagades de contractes de baixa: Número de factures'
        'COB5',  --'R1: Número de factures',
        'COB7',  --'Pobresa: Número de factures',
        'COB10', --'Fraccionament: Número de factures',
        'COB14' --'Monitori: Número de factures'
      ) then 'Import factures en procediment de tall: Número'
      else NULL
    end as kpi_membership
    from {{ref('kpis_raw')}}
)
select
  create_date as kpi_date,
  create_date,
  case
    when kpi_membership = 'Import factures en procediment de tall: Import' then 'DBT1'
    when kpi_membership = 'Import factures en procediment de tall: Número' then 'DBT2'
    else NULL
  end as code,
  kpi_membership as name,
  case
    when kpi_membership = 'Import factures en procediment de tall: Import' then 'FACTURES EN PROCEDIMENT DE TALL:  import = (COB2+COB3) - COB15 - COB4 - COB6 - (COB8+COB9) - 13'
    when kpi_membership = 'Import factures en procediment de tall: Número' then 'FACTURES EN PROCEDIMENT DE TALL:  numero = COB1 - COB16 - COB5 - COB7 - COB10 - COB14'
    else NULL
  end as description,
  sum(multiplier*value) as value
from combinable_kpis
where kpi_membership is not null
group by create_date, kpi_membership
