{{ config(materialized='view') }}

select name, value, description, kpi_date, create_date
from {{ref('kpis_raw')}}
union all
select name, value, description, kpi_date, create_date
from {{ ref('kpis_combined')}}


