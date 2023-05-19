{{ config(materialized='table') }}

select *
from {{source('erp_transaccional', 'giscedata_polissa')}}
