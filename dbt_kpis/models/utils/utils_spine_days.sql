{{ config(materialized='view') }}

with limits as (
select
  '2011-08-25'::date as data_min, --data del primer contracte a la coope
  current_date::date as data_max
), spine as (
  select generate_series(l.data_min, l.data_max, interval '1 day')::Date as at_date
  from limits as l
  )
select * from spine