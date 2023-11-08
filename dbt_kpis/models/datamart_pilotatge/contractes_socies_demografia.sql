{{ config(materialized='view') }}
with last_rpa as (
	select partner_id, max(create_date) as last_create_date
	from {{source('erp', 'res_partner_address')}}
	group by partner_id
), current_adress as (
	select last_rpa.partner_id, full_rpa.phone, full_rpa.mobile, full_rpa.email
	from last_rpa as last_rpa
	left join {{source('erp', 'res_partner_address')}} as full_rpa
		on last_rpa.partner_id = full_rpa.partner_id
		and last_rpa.last_create_date = full_rpa.create_date
)
select
	rp.id as res_partner_id,
	SPLIT_PART(rp.name, ', ',2) as nom_de_pila,
	rp.name as nom_complet,
	ca.phone,
	ca.mobile,
	ca.email,
	rp.lang,
	gp.titular, -- res_partner id
	gpt.name as tarifa,
	gp.cnae,
	gp.potencia as potencia,
	gp.autoconsumo,
	gp.tipus_vivenda,
	CASE WHEN gp.active = TRUE and gp.data_baixa is null THEN TRUE ELSE FALSE END as contracte_actiu,
    CASE WHEN ss.data_baixa_soci is null THEN TRUE ELSE FALSE END as socia_activa,
	gp.data_alta - NOW() as edat_som,
	rm.name as municipi,
	rc.name as comarca,
	rcs.name as provincia,
	rca.name as ccaa
from {{source('erp', 'somenergia_soci')}} as ss
left join {{source('erp', 'giscedata_polissa')}} as gp on ss.partner_id = gp.titular
left join {{source('erp', 'giscedata_polissa_tarifa')}} as gpt on gpt.id = gp.tarifa
left join {{source('erp', 'res_partner')}} as rp on rp.id = gp.titular
left join current_adress as ca on ca.partner_id = rp.id
left join {{source('erp', 'giscedata_cups_ps')}} as gpc on gp.cups = gpc.id
left join {{source('erp', 'res_municipi')}} as rm on rm.id = gpc.id_municipi
left join {{source('erp', 'res_comarca')}} as rc on rm.comarca = rc.id
left join {{source('erp', 'res_country_state')}} as rcs on rm.state = rcs.id
left join {{source('erp', 'res_comunitat_autonoma')}} as rca on rcs.comunitat_autonoma = rca.id