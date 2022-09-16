{{ config(materialized='view') }}

SELECT
    es_baixa,
    data_baixa,
    data_alta
FROM {{ref('erp_somenergia_soci')}}


