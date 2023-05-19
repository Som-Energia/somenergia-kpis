{{ config(materialized='view') }}

{# this adds a row everytime there's a change on the combination of categories. It will, hence, holes,
which you'd have to backfill to fisplay in a barplot. On a line graph, tho, it would work as expected.
If you want a value on every day, then you should use contracts_spine. #}

with auto_contracts as (
    SELECT
        data_alta,
    		data_baixa,
    		gpt.name as tarifa,
    		CASE
    			WHEN autoconsumo ilike '00' THEN 'Noauto'
    			WHEN autoconsumo ilike '41' THEN 'Individual'
    			WHEN autoconsumo ilike '42' THEN 'Collectiu'
    			ELSE 'Altres'
    		END AS tipus,
        autoconsumo not in ('41', '42') or data_alta_autoconsum is not null as is_auto_coherent
      FROM {{ ref('raw_somenergia_contract') }} as gp
      left join {{ ref('raw_somenergia_contract_tariff') }} as gpt on gp.tarifa = gpt.id
),
sane_contracts as (
  select * from auto_contracts
  where is_auto_coherent and data_alta is not null
),
contracts as (
    select
        data_alta as data,
        1 as n_casos,
        tarifa,
        tipus
    from sane_contracts
    union all
    select
        data_baixa + 1 as data,
        -1 as n_casos,
        tarifa,
        tipus
    from sane_contracts
    where data_baixa is not null
)
select
  data,
  tarifa,
  tipus,
  sum(sum(n_casos)) over (partition by tarifa,tipus order by data) as num_contracts
from contracts
group by data, tarifa, tipus
order by data desc