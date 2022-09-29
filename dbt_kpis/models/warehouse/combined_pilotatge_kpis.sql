{{ config(materialized='view') }}

-- TODO table the combination
with combinable_kpis as (
    select
    create_date,
    value,
    case
      when name like 'Saldo pendent' or name like 'Número de factures pendents' then +1
      else -1
    end as multiplier,
    case
      when name in (
        'Saldo pendent', -- positive
        'Factures impagades de contractes de baixa: Import',
        'R1: Import',
        'Pobresa: Import',
        'Fraccionament: Import',
        'Monitori: Import'
      ) then 'Import factures en procediment de tall: Import'
      when name in (
        'Número de factures pendents', -- positive
        'Factures impagades de contractes de baixa: Número de factures',
        'R1: Número de factures',
        'Pobresa: Número de factures',
        'Fraccionament: Número de factures',
        'Monitori: Número de factures'
      ) then 'Import factures en procediment de tall: Número'
      else NULL
    end as kpi_membership
    from {{ref('pilotatge_kpis_model')}}
)
select
  create_date as kpi_date,
  create_date,
  kpi_membership as name,
  NULL as description,
  sum(multiplier*value) as value
from combinable_kpis
where kpi_membership is not null
group by create_date, kpi_membership
