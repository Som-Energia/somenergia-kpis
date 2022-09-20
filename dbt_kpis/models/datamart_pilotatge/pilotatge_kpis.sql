{{ config(materialized='view') }}

select
    name, value, description, kpi_date
from {{ref('pilotatge_kpis_model')}}


