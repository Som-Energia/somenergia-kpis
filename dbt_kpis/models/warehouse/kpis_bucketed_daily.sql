{{ config(materialized='view') }}

{# We assume that create_date is the batch id and that each batch computes all kpis (imputing same create_date)#}

with unique_kpis as (

    SELECT max(create_date) as create_date
    from {{ref('kpis_with_derived')}}
    group by date_trunc('day',create_date)
)
SELECT
    kpis.*
FROM unique_kpis AS ukpis
LEFT JOIN {{ref('kpis_with_derived')}} as kpis
ON
    ukpis.create_date = kpis.create_date
