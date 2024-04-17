{{ config(materialized='view') }}
with last_rpa as (
	select partner_id, max(id) as last_id
	from {{source('erp', 'res_partner_address')}}
	group by partner_id
), current_adress as (
	select full_rpa.*
	--last_rpa.partner_id, full_rpa.phone, full_rpa.mobile, full_rpa.email
	from last_rpa as last_rpa
	left join {{source('erp', 'res_partner_address')}} as full_rpa
		on last_rpa.partner_id = full_rpa.partner_id
		and last_rpa.last_id = full_rpa.id
), ine_noms as (
	SELECT nom, frequencia, genere,
		rank() OVER (PARTITION BY nom ORDER BY frequencia DESC)
	FROM {{source('prod', 'ine_nombres_sexo')}}
)
select
	rp.ref as partner_ref,
	rp.id as res_partner_id,
	SPLIT_PART(rp.name, ', ',2) as nom_de_pila,
	rp.name as nom_complet,
	rp.vat as partner_vat,
	CASE
		WHEN rp.vat ~ '[0-9]{8}[TRWAGMYFPDXBNJZSQVHLCKE]' THEN 'Persona física'
		WHEN rp.vat ~ '[XYZ][0-9]{7}' THEN 'Persona física estrangera'
		WHEN rp.vat ~ 'A[0-9]{7}' THEN 'Societat anònima'
		WHEN rp.vat ~ 'B[0-9]{7}' THEN 'Societat de responsabilitat limitada'
		WHEN rp.vat ~ 'C[0-9]{7}' THEN 'Societat col·lectiva'
		WHEN rp.vat ~ 'D[0-9]{7}' THEN 'Societat comanditària'
		WHEN rp.vat ~ 'E[0-9]{7}' THEN 'Comunitat de béns i herència jacent'
		WHEN rp.vat ~ 'F[0-9]{7}' THEN 'Societat cooperativa'
		WHEN rp.vat ~ 'G[0-9]{7}' THEN 'Associació'
		WHEN rp.vat ~ 'H[0-9]{7}' THEN 'Comunitat de propietaris en règim de propietat horitzontal'
		WHEN rp.vat ~ 'J[0-9]{7}' THEN 'Societat civil, amb o sense personalitat jurídica'
		WHEN rp.vat ~ 'N[0-9]{7}' THEN 'Entitat estrangera'
		WHEN rp.vat ~ 'P[0-9]{7}' THEN 'Corporació Local'
		WHEN rp.vat ~ 'Q[0-9]{7}' THEN 'Organisme públic'
		WHEN rp.vat ~ 'R[0-9]{7}' THEN 'Congregació i institució religiosa'
		WHEN rp.vat ~ 'S[0-9]{7}' THEN 'Òrgan de l''Administració de l''Estat i de les Comunitats Autònomes'
		WHEN rp.vat ~ 'U[0-9]{7}' THEN 'Unió Temporal d''Empreses'
		WHEN rp.vat ~ 'V[0-9]{7}' THEN 'Altres tipus no definits en la resta de claus'
		WHEN rp.vat ~ 'W[0-9]{7}' THEN 'Establiment permanent d''entitat no resident en territori espanyol'
		ELSE 'Altres'
	END as personalitat_juridica,
	ca.id as current_adress_id,
	ca.create_date as current_adress_create_date,
	ca.phone,
	ca.mobile,
	ca.email,
	partner_rm.name as partner_municipi,
	partner_rc.name as partner_comarca,
	partner_rcs.name as partner_provincia,
	partner_rca.name as partner_ccaa,
	ine_genere.genere as genere,
	rp.lang,
	gpt.name as tarifa,
	gp.id as polissa_id,
	gp.cups,
	gp.cnae,
	gp.potencia as potencia,
	gp.autoconsumo,
	gp.tipus_vivenda,
	gp.soci as soci_padri,
	CASE WHEN gp.active = TRUE and gp.data_baixa is null THEN TRUE ELSE FALSE END as contracte_actiu,
    CASE WHEN ss.data_baixa_soci is null THEN TRUE ELSE FALSE END as socia_activa,
	ROUND(ABS(EXTRACT(days FROM (gp.data_alta - NOW()))/365.25)::decimal,2) as anys_antiguitat,
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
left join {{source('erp', 'res_municipi')}} as partner_rm on partner_rm.id = ca.id_municipi
left join {{source('erp', 'res_comarca')}} as partner_rc on partner_rm.comarca = partner_rc.id
left join {{source('erp', 'res_country_state')}} as partner_rcs on partner_rm.state = partner_rcs.id
left join {{source('erp', 'res_comunitat_autonoma')}} as partner_rca on partner_rcs.comunitat_autonoma = partner_rca.id
left join (SELECT * FROM ine_noms WHERE rank = 1) as ine_genere
	on LOWER(ine_genere.nom) = translate(LOWER(SPLIT_PART(rp.name, ', ',2)), 'áàéèíòóú', 'aaeeioouu')
