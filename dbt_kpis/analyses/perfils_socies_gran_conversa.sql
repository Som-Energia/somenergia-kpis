{{ config(materialized='table') }}


{# WIP https://trello.com/c/RwELqTHP/224-informe-perfils-cooperativa #}

select rca.name as CCAA, count(*) as contractes
from giscedata_polissa as gp
left join giscedata_cups_ps as gpc on gp.cups = gpc.id
left join res_municipi as rm on rm.id = gpc.id_municipi
left join res_country_state as rcs on rm.state = rcs.id
left join res_comunitat_autonoma as rca on rcs.comunitat_autonoma = rca.id
where gp.active = TRUE and gp.data_baixa is null
group by rca.name

select TRUE as es_soci, ss.data_baixa_soci, ss.partner_id
from somenergia_soci as ss
left join giscedata_polissa as gp
where ss.data_baixa_soci is null


select * from giscedata_polissa as gp limit 10

-- -- categories demandes
-- Gènere
-- Territori - comarca?
-- categoria potencia
-- auto o no
-- edat de som, quant fa que són a la coope?
--------------------

select titular, -- res_partner id
	--string_agg(tarifa, ', ') as tarifes,
	--string_agg(cnae, ', ') as cnaes,
	max(potencia) as potencia_max,
	sum(CASE WHEN autoconsumo = '00' THEN 0 ELSE 1 END) as te_auto,
	sum(CASE WHEN tipus_vivenda = 'habitual' THEN 0 ELSE 1 END) as te_segona_residencia,
	min(data_alta) - NOW() as edat_som
from giscedata_polissa as gp
where gp.active = TRUE and gp.data_baixa is null
group by titular
