
{% set show_query %}
select * from {{ ref('pilotatge_kpis') }}
order by create_date desc limit 10
{% endset %}

{% set results = run_query(show_query) %}

{# jinja compile errors on jinja commands that expect results #}
{# see https://docs.getdbt.com/reference/dbt-jinja-functions/execute #}
{% if execute %}
{% do results.print_table() %}
{{ log("", info=True) }}
{% endif %}

{# jinja expects something to put inside #}
{{ show_query }}