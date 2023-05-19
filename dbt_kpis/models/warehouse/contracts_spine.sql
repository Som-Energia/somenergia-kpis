{{ config(materialized='view') }}

{# note that autoconsum doesn't have historic in giscedata_polissa, this query is too slow (it's generating)#}

with spine AS
(
  select
  generate_series(
        '2020-01-01',
        CURRENT_DATE-1,
        '1 day'::interval
      ) as dia
),
auto_contracts as (
    SELECT
        data_alta,
		data_baixa,
		gpt.name as tarifa,
		CASE
			WHEN autoconsumo ilike '00' THEN 'Noauto'
			WHEN autoconsumo ilike '41' THEN 'Individual'
			WHEN autoconsumo ilike '42' THEN 'Collectiu'
			ELSE 'Altres'
		END AS tipus_auto,
		autoconsumo not in ('41', '42') or data_alta_autoconsum is not null as is_auto_coherent
	FROM giscedata_polissa as gp
	left join giscedata_polissa_tarifa as gpt on gp.tarifa = gpt.id
)
select
 dia,
 count(*) as contractes_actius,
 tipus_auto,
 tarifa
 from spine
left join auto_contracts on spine.dia between data_alta and coalesce(data_baixa, '2050-01-01')
GROUP BY dia, tipus, tarifa
ORDER BY dia desc