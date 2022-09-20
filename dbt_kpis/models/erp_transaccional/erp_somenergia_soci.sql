{{ config(materialized='table') }}

select es_baixa, data_baixa, data_alta
from {{ref('erp_somenergia_soci_dirty')}}
WHERE socia_coherent = TRUE
and data_baixa_coherent and data_alta_coherent
