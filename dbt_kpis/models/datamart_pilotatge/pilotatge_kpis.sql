{{ config(materialized='view') }}

select *
from {{ ref('kpis_bucketed_daily')}}


