{{ config(materialized='table') }}

with spine as (
  select *
  from {{ ref("utils_spine_days") }}
),
count_contracts as (
  select
    spine.at_date,
    cups.system_code,
    cups.dso_tariff,
    cups.comer_tariff,
    count(cups.cups_20) as n_contracts
  from spine
    left join {{ ref("raw_erp__historical_contract_terms") }} as cups
      on spine.at_date between cups.start_date and cups.end_date
  group by spine.at_date, cups.system_code, cups.dso_tariff, cups.comer_tariff
  order by spine.at_date desc, cups.system_code asc
)
select * from count_contracts
