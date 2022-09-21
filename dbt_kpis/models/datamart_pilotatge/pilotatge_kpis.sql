{{ config(materialized='view') }}

select name, value, description, kpi_date, create_date
from {{ref('pilotatge_kpis_model')}}
union all
select name, value, description, kpi_date, create_date
from {{ ref('combined_pilotatge_kpis')}}


