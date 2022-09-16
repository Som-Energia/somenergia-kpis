{{ config(materialized='view') }}


with altes as (

    select
        'alta' as cas,
        data_alta as data,
        1 as n_casos,
        1 as n_casos_sum
    from {{ref('altes_socies')}}

), baixes as (

    select
        'baixa' as cas,
        data_baixa as data,
        1 as n_casos,
        -1 as n_casos_sum
    from {{ref('baixes_socies')}}
)

select *
from altes
union all
select *
from baixes
order by data desc


