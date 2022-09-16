{{ config(materialized='view') }}

select
    data_alta
from {{ref('erp_somenergia_soci')}}


