{{ config(materialized='table') }}

SELECT
    baixa as es_baixa,
    data_baixa_soci as data_baixa
FROM {{source('datalake_erp', 'somenergia_soci')}}
