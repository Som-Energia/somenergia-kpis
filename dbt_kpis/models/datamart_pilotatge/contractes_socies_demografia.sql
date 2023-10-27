
select
	SPLIT_PART(rp.name, ', ',2) as nom_de_pila,
	rp.lang,
	ss.partner_id as soci,
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
left join {{source('erp', 'giscedata_cups_ps')}} as gpc on gp.cups = gpc.id
left join {{source('erp', 'res_municipi')}} as rm on rm.id = gpc.id_municipi
left join {{source('erp', 'res_comarca')}} as rc on rm.comarca = rc.id
left join {{source('erp', 'res_country_state')}} as rcs on rm.state = rcs.id
left join {{source('erp', 'res_comunitat_autonoma')}} as rca on rcs.comunitat_autonoma = rca.id