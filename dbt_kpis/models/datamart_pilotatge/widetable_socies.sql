{{ config(materialized='table') }}

with somenergia_soci as (

    SELECT es_baixa, data_baixa
    FROM {{ref('erp_somenergia_soci')}}

)
select *
from somenergia_soci

