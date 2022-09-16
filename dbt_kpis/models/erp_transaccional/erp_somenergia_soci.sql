{{ config(materialized='table') }}

with socies as (
SELECT
    baixa as es_baixa,
    data_baixa_soci as data_baixa,
    create_date as data_alta,
    case
        when data_baixa_soci is not NULL and baixa = TRUE then TRUE
        when data_baixa_soci is NULL and baixa = FALSE then TRUE
        when data_baixa_soci is NULL and baixa = TRUE then FALSE
        when data_baixa_soci is not NULL and baixa = FALSE then FALSE
        when baixa = NULL then FALSE
    end as socia_coherent
FROM {{source('datalake_erp', 'somenergia_soci')}}
)

select *
from socies
WHERE socia_coherent = TRUE
