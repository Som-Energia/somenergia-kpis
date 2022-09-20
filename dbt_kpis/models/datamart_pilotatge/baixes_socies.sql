{{ config(materialized='view') }}

select
    data_baixa
from {{ref('erp_somenergia_soci')}}
where es_baixa = TRUE


