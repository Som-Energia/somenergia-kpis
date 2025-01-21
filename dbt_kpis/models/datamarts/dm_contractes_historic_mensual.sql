{{
  config(
    materialized='table',
    indexes=[
      {
        'columns': ['month_date', 'system_code', 'dso_tariff', 'comer_tariff'],
        'columns': ['month_date', 'dso_tariff', 'comer_tariff'],
      }
    ]
  )
}}

with base as (

  select
      {{ dbt_utils.star(from=ref('int_contractes__historical')) }},
      date_trunc('month', at_date) as month_date
  from {{ ref('int_contractes__historical') }}
),

last_values_monthly as (
  select
    at_date,
    month_date::date,
    system_code,
    dso_tariff,
    comer_tariff,
    first_value(n_contracts) over (
        partition by (month_date, system_code, dso_tariff, comer_tariff)
	      order by at_date asc
      )
    as n_contracts_at_month_end
  from base
)
select
    distinct on
      (month_date,
      system_code,
      dso_tariff,
      comer_tariff)
    month_date,
    system_code,
    dso_tariff,
    comer_tariff,
    n_contracts_at_month_end
from last_values_monthly
order by month_date desc
