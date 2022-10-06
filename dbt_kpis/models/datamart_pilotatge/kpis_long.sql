{{ config(materialized='view') }}


with long as (

{{ dbt_utils.unpivot(
      relation=ref('kpis_bucketed_daily'),
      cast_to='float',
      exclude=['create_date'],
      field_name='code',
      value_name='value'
    ) }}
)

SELECT
  long.create_date,
  descr.name,
  long.value,
  long.create_date - descr.day_offset as kpi_date,
  descr.code,
  descr.description
FROM long
LEFT JOIN {{ref('kpis_description')}} as descr
ON descr.code = long.code
where descr.code not in ('cob2', 'cob3', 'cob8', 'cob9') --parcial KPIs Pilotatge no interested
order by create_date desc, descr.code