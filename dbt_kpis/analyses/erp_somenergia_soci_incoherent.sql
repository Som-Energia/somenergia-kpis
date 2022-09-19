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
    data_baixa_soci between '2009-01-01' and now() + interval '1 day' as data_baixa_coherent,
    create_date between '2009-01-01' and now() + interval '1 day' as data_alta_coherent
FROM {{source('datalake_erp', 'somenergia_soci')}}
where socia_coherent = FALSE or data_baixa_coeherent = FALSE or data_alta_coherent = FALSE
LIMIT 1000