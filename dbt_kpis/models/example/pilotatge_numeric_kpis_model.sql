{{ config(materialized='table', enabled=false) }}

with kpisvalues as (

    SELECT pkd.id, pkd.name, pkd.description, pfk.value, pfk.create_date
    FROM {{ source('public', 'pilotatge_kpis_description')}} as pkd
    LEFT JOIN {{ source('public', 'pilotatge_float_kpis')}} as pfk
    ON pkd.id = pfk.kpi_id
    where type_value = 'float'
    UNION
    SELECT pkd.id, pkd.name, pkd.description, pik.value, pik.create_date
    FROM pilotatge_kpis_description as pkd
    LEFT JOIN {{ source('public', 'pilotatge_int_kpis')}} as pik
    ON pkd.id = pik.kpi_id
    where type_value = 'int'
    order by id

),
kpisvaluesrecoded as (

	select
		*,
		CASE
			WHEN name IN ('Saldo pendent_a', 'Saldo pendent_b') THEN 'Saldo pendent'
			WHEN name IN ('Fraccionament: Import_a', 'Fraccionament: Import_b') THEN 'Fraccionament: Import'
			ELSE name
		END as clean_name
	from kpisvalues
)
select clean_name as name, sum(value) as value, string_agg(description, ' , ') as description, create_date
from kpisvaluesrecoded
group by clean_name, create_date

