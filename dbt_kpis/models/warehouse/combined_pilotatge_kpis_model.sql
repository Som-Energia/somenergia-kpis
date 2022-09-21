{{ config(materialized='view') }}

-- select
--     *
-- from crosstab('select kpi_date, name, value from {{ref("pilotatge_kpis_model")}} order by 1,2')
-- as ct(kpi_date timestamptz, kpi1 numeric, kpi2 numeric, kpi3 numeric, kpi4 numeric, kpi5 numeric, kpi6 numeric, kpi7 numeric, kpi8 numeric)



select kpi_date, {{ dbt_utils.pivot('name', dbt_utils.get_column_values(ref('pilotatge_kpis_model'), 'name')) }} from {{ ref('pilotatge_kpis_model') }} group by kpi_date