{{ config(materialized='table') }}


select * from {{ref('erp_somenergia_soci_dirty')}}
where socia_coherent = FALSE or data_baixa_coeherent = FALSE or data_alta_coherent = FALSE
order by data_alta desc
LIMIT 1000