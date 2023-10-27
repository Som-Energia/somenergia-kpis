{# WIP https://trello.com/c/RwELqTHP/224-informe-perfils-cooperativa #}

-- -- categories demandes
-- Gènere
-- Territori - comarca?
-- categoria potencia
-- auto o no
-- edat de som, quant fa que són a la coope?
--------------------

with polissa as (
	select titular, -- res_partner id
		ss.partner_id as soci,
		--string_agg(tarifa, ', ') as tarifes,
		--string_agg(cnae, ', ') as cnaes,
		max(gp.potencia) as potencia_max,
		sum(CASE WHEN gp.autoconsumo = '00' THEN 0 ELSE 1 END) as te_auto,
		sum(CASE WHEN gp.tipus_vivenda = 'habitual' THEN 0 ELSE 1 END) as te_segona_residencia,
		sum(CASE WHEN gp.active = TRUE and gp.data_baixa is null THEN 1 ELSE 0 END) as contractes_actius,
		min(gp.data_alta) - NOW() as edat_som
	from somenergia_soci as ss
	left join giscedata_polissa as gp on ss.partner_id = gp.titular
	--where gp.active = TRUE and gp.data_baixa is null
	where ss.data_baixa_soci is null
	group by titular, ss.partner_id
)
select
	polissa.*,
	SPLIT_PART(rp.name, ', ',2) as nom_de_pila,
	rp.lang
from polissa as polissa
left join res_partner as rp on rp.id = polissa.titular


select
	SPLIT_PART(rp.name, ', ',2) as nom_de_pila,
	rp.lang,
	ss.partner_id as soci,
	gp.titular, -- res_partner id
	--gp.tarifa,
	gpt.name as tarifa,
	gp.cnae,
	gp.potencia as potencia,
	gp.autoconsumo,
	gp.tipus_vivenda,
	CASE WHEN gp.active = TRUE and gp.data_baixa is null THEN 1 ELSE 0 END as contracte_actiu,
	gp.data_alta - NOW() as edat_som,
	rm.name as municipi,
	rc.name as comarca,
	rcs.name as provincia,
	rca.name as ccaa
from somenergia_soci as ss
left join giscedata_polissa as gp on ss.partner_id = gp.titular
left join giscedata_polissa_tarifa as gpt on gpt.id = gp.tarifa
left join res_partner as rp on rp.id = gp.titular
left join giscedata_cups_ps as gpc on gp.cups = gpc.id
left join res_municipi as rm on rm.id = gpc.id_municipi
left join res_comarca as rc on rm.comarca = rc.id
left join res_country_state as rcs on rm.state = rcs.id
left join res_comunitat_autonoma as rca on rcs.comunitat_autonoma = rca.id
--where gp.active = TRUE and gp.data_baixa is null
where ss.data_baixa_soci is null













