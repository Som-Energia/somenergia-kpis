{{ config(materialized='table') }}

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
    end as socia_coherent,
    case when data_baixa_soci is NULL then TRUE
        when data_baixa_soci is not NULL and data_baixa_soci between '2009-01-01' and now() + interval '1 day' then TRUE
        ELSE FALSE
    end as data_baixa_coherent,
    create_date between '2009-01-01' and now() + interval '1 day' as data_alta_coherent
FROM {{source('erp', 'somenergia_soci')}}
