{{ config(materialized='view') }}

select
    create_date,
    create_date at time zone 'Europe/Zurich' as create_date_local,
    {{ pivot(column='name', names=dbt_utils.get_column_values(table=ref('kpis_long'), column='name'), value_column='value', agg='max') }}
from {{ref('kpis_long')}}
group by create_date
order by create_date asc


