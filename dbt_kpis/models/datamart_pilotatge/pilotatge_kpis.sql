{{ config(materialized='view') }}

select
    *
from {{ref('pilotatge_kpis_model')}}


