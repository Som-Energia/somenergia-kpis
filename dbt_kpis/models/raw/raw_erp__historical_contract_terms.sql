{{
  config(
    materialized = 'table',
    indexes=[
      {'columns': ['cups_20'], 'type': 'btree'},
      {'columns': ['start_date', 'end_date', 'system_code'], 'type': 'btree'},
    ]
    )
}}

with subsystem_timezone as (
  select t.*
  from (
    values
    (1, 'CE', 'Ceuta', 'CEU', 'Europe/Madrid'),
    (2, 'FL', 'Fuerteventura-Lanzarote', 'CAN', 'Atlantic/Canary'),
    (3, 'GC', 'Gran Canaria', 'CAN', 'Atlantic/Canary'),
    (4, 'HI', 'Hierro', 'CAN', 'Atlantic/Canary'),
    (5, 'SB', 'Sistema Balear', 'BAL', 'Europe/Madrid'),
    (6, 'LG', 'La Gomera', 'CAN', 'Atlantic/Canary'),
    (7, 'PA', 'La Palma', 'CAN', 'Atlantic/Canary'),
    (8, 'SB', 'Sistema Balear', 'BAL', 'Europe/Madrid'),
    (9, 'ML', 'Melilla', 'MEL', 'Europe/Madrid'),
    (10, 'PE', 'Península', 'PEN', 'Europe/Madrid'),
    (11, 'TF', 'Tenerife', 'CAN', 'Atlantic/Canary')
  ) as t (
    id_subsystem_erp,
    subsystem_code,
    subsystem_erp_name,
    system_code,
    timezone
  )
),

cups as (
  select
    cups_ps.id as cups_id,
    cups_ps.name as cups_name,
    cups_ps.distribuidora_id,
    res_partner.name as distribuidora_name,
    cups_ps.id_municipi as municipality_id,
    municipi.name as municipality_name,
    municipi.climatic_zone,
    municipi.id is not null as is_valid_municipality,
    municipi.ine as municipality_ine_code,
    subsystems.id as subsystem_id,
    subsystems.num_code as subsystem_num_code,
    subsystems.code as subsystem_code,
    subsystems.description as subsystem_name,
    seed_subsystem.system_code,
    seed_subsystem.timezone,
    cups_ps.name ~ '^(ES)[0-9]{4}[0-9]{12}[A-Z]{2}[0-9]?[FTPXC]?$' as is_valid_cups
  from {{ source('erp', "giscedata_cups_ps") }} as cups_ps
    left join {{ source('erp', "res_municipi") }} as municipi on cups_ps.id_municipi = municipi.id
    left join {{ source('erp', "res_partner") }} as res_partner on cups_ps.distribuidora_id = res_partner.id
    left join {{ source('erp', "res_subsistemas_electricos") }} as subsystems on municipi.subsistema_id = subsystems.id
    left join subsystem_timezone as seed_subsystem on municipi.subsistema_id = seed_subsystem.id_subsystem_erp
),

base_erp as (
  select
    mod.cups as cups_ps_id,
    dn.cups_name as cups,
    mod.data_inici as start_date,
    mod.data_final as end_date,
    mod.data_inici::timestamp at time zone coalesce(
      dn.timezone, 'Europe/Madrid'
    ) as start_ts_aware,
    pol.name as contract,
    usr.name as contract_holder,
    replace(t.name, '.', '') as dso_tariff,
    pr.name as comer_tariff_name,
    mod.potencia as max_power,
    mod.potencies_periode as power_periods_kw,
    mod.autoconsumo as auto_type,
    pol.tipo_medida as mesuring_point_types,
    pol.state as polissa_state,
    cnae.name as contract_cnae,
    dn.municipality_id,
    dn.municipality_ine_code,
    dn.municipality_name,
    dn.distribuidora_id as dso_id,
    dn.distribuidora_name as dso_name,
    {# we assume latest version (polissa) was also correct in the past #}
    dn.subsystem_id,
    dn.subsystem_name,
    dn.subsystem_num_code,
    dn.subsystem_code,
    dn.system_code,
    dn.timezone,
    dn.climatic_zone,
    mod.coeficient_k,
    mod.coeficient_d,
    left(dn.cups_name, 20) as cups_20,
    coalesce(dn.is_valid_cups, false) as is_valid_cups,
    (mod.data_final::timestamp + interval '1 day') at time zone coalesce(dn.timezone, 'Europe/Madrid') as end_ts_aware,
    soci.partner_id is not null as is_member_now,
    case
      when mod.mode_facturacio = 'atr' then 'fixed_fee'
      when mod.mode_facturacio = 'index' then 'indexed'
      else 'strange_fact'
    end as tariff_type,
    case
      when pr.name ~ '^(Indexada Empresa.*|\[TE\].*|Mancomunidad.*|.*ESMASA|Aguas.*|Aj\..*|BUFF.*)'
        then 'eie'
      when pr.name ~ '^(.*SOM.*|.*alzado.*|.*doscomptadors.*|Indexada.*)'
        then 'web'
      else 'error'
    end as client_type,
    case
      when mod.mode_facturacio = 'atr' and pr.name ~ '^(Indexada Empresa.*|\[TE\].*|Mancomunidad.*|.*ESMASA|Aguas.*|Aj\..*|BUFF.*)'
        then 'fixa_eie'
      when mod.mode_facturacio = 'index' and pr.name ~ '^(Indexada Empresa.*|\[TE\].*|Mancomunidad.*|.*ESMASA|Aguas.*|Aj\..*|BUFF.*)'
        then 'index_eie'
      when mod.mode_facturacio = 'atr' and pr.name ~ '^(.*SOM.*|.*alzado.*|.*doscomptadors.*|Indexada.*)'
        then 'fixa_web'
      when mod.mode_facturacio = 'index' and pr.name ~ '^(.*SOM.*|.*alzado.*|.*doscomptadors.*|Indexada.*)'
        then 'index_web'
      else 'error'
    end as comer_tariff,
    case
      when 0.0 <= mod.potencia and mod.potencia <= 3.0 then '0_3'
      when 3.0 < mod.potencia and mod.potencia <= 5.0 then '>3_5'
      when 5.0 < mod.potencia and mod.potencia <= 7.0 then '>5_7'
      when 7.0 < mod.potencia and mod.potencia <= 10.0 then '>7_10'
      when 10.0 < mod.potencia and mod.potencia <= 15.0 then '>10_15'
      when 15.0 < mod.potencia and mod.potencia <= 20.0 then '>15_20'
      when 20.0 < mod.potencia and mod.potencia <= 30.0 then '>20_30'
      when 30.0 < mod.potencia and mod.potencia <= 50.0 then '>30_50'
      when 50.0 < mod.potencia and mod.potencia <= 70.0 then '>50_70'
      when mod.potencia > 70.0 then '>70_max'
    end as categoria_potencia,
    coalesce(dn.is_valid_municipality, false) as is_valid_municipality,
    mod.write_date at time zone 'UTC' as erp_updated_at
  from {{ source('erp', 'giscedata_polissa_modcontractual') }} as mod
    left join
      {{ source('erp', 'giscedata_polissa_tarifa') }} as t
      on mod.tarifa = t.id
    left join
      {{ source('erp', 'giscedata_polissa') }} as pol
      on mod.polissa_id = pol.id
    left join {{ source('erp', 'res_partner') }} as usr on mod.titular = usr.id
    left join
      {{ source('erp', 'product_pricelist') }} as pr
      on mod.llista_preu = pr.id
    left join
      {{ source('erp', 'somenergia_soci') }} as soci
      on usr.id = soci.partner_id
    left join cups as dn on mod.cups = dn.cups_id
    left join
      {{ source('erp', 'giscemisc_cnae') }} as cnae
      on mod.cnae = cnae.id
  where pol.state not in ('cancelada', 'esborrany')
),

valid_cups as (
  select *
  from base_erp
  where cups_20 is not null or is_valid_cups is true
),

fix_overlaps as (
  select
    *,
    case
      when cups_20 = 'ES0552000000005006TS' and start_date = '2021-06-01' and end_date = '2022-08-04' then '2022-07-20'::date
      when cups_20 = 'ES0031406002830011NN' and start_date = '2021-06-01' and end_date = '2023-04-25' then '2023-04-24'::date
      when cups_20 = 'ES0630000000000188HY' and start_date = '2021-06-01' and end_date = '2022-10-09' then '2022-09-30'::date
      else end_date
    end as end_date_corregit
  from valid_cups
),

historical_contract_terms as (
  select
    cups_20,
    is_valid_cups,
    start_date,
    end_date_corregit as end_date,
    dso_tariff,
    comer_tariff_name,
    tariff_type,
    comer_tariff,
    categoria_potencia,
    power_periods_kw,
    auto_type,
    subsystem_code,
    system_code,
    coeficient_k,
    coeficient_d
  from
    fix_overlaps
)

select * from historical_contract_terms
