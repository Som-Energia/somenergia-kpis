{{ config(materialized='view') }}

select code, name, value, description, kpi_date, create_date
from {{ref('kpis_raw')}}
union all
select code, name, value, description, kpi_date, create_date
from {{ ref('kpis_combined')}}


