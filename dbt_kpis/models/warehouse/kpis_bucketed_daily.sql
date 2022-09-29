{{ config(materialized='view') }}

with unique_kpis as (

    SELECT max(create_date) as create_date, name
    from {{ref('kpis')}}
    group by date_trunc('day',create_date), name
)
SELECT
    kpis.*
FROM unique_kpis AS ukpis
LEFT JOIN {{ref('kpis')}} as kpis
ON
    ukpis.create_date = kpis.create_date
    AND ukpis.name = kpis.name
