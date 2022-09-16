{{ config(materialized='table') }}

SELECT
    baixa,
    data_baixa_soci as data_baixa,
    create_date as data_alta,
    case
        when data_baixa_soci is not NULL and baixa = TRUE then TRUE
        when data_baixa_soci is NULL and baixa = FALSE then FALSE
        when data_baixa_soci is NULL and baixa = TRUE then NULL
        when data_baixa_soci is not NULL and baixa = FALSE then NULL
        when baixa = NULL then NULL
    end as socia_coherent,
FROM {{source('datalake_erp', 'somenergia_soci')}}
where socia_coherent = FALSE