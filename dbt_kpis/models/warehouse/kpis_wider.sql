{{ config(materialized='view') }}

select
    create_date,
    {{ pivot(column='code', names=dbt_utils.get_column_values(table=ref('kpis_raw'), column='code'), value_column='value', agg='max') }}
from {{ref('kpis_raw')}}
group by create_date
