{{ config(materialized='view') }}

with kpisvalues as (

    SELECT pkd.id, pkd.name, pkd.description, pkd.freq, pfk.value, pfk.create_date
    FROM {{ source('erp_operational', 'pilotatge_kpis_description')}} as pkd
    LEFT JOIN {{ source('erp_operational', 'pilotatge_float_kpis')}} as pfk
    ON pkd.id = pfk.kpi_id
    where type_value = 'float'
    UNION ALL
    SELECT pkd.id, pkd.name, pkd.description, pkd.freq, pik.value, pik.create_date
    FROM {{ source('erp_operational', 'pilotatge_kpis_description') }} as pkd
    LEFT JOIN {{ source('erp_operational', 'pilotatge_int_kpis')}} as pik
    ON pkd.id = pik.kpi_id
    where type_value = 'int'
    order by id

),
kpisvaluesrecoded as (

	select
		value,
        description,
        create_date,
        case
            when freq = '__7_days_ago__' then interval '7 days'
            when freq = '__3_days_ago__' then interval '3 days'
            when freq = '__yesterday__' then interval '1 days'
            else interval '0 days'
        end as day_offset,
		CASE
			WHEN name IN ('Saldo pendent_a', 'Saldo pendent_b') THEN 'Saldo pendent'
			WHEN name IN ('Fraccionament: Import_a', 'Fraccionament: Import_b') THEN 'Fraccionament: Import'
			ELSE name
		END as clean_name
	from kpisvalues
)
select clean_name as name, sum(value) as value, string_agg(description, ' , ') as description, create_date - day_offset as kpi_date
from kpisvaluesrecoded
group by clean_name, kpi_date
