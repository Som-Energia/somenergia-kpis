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
    CASE WHEN ss.data_baixa_soci is null and ss.id is not null THEN TRUE ELSE FALSE END as socia_activa,
	ROUND(ABS(EXTRACT(days FROM (gp.data_alta - NOW()))/365.25)::decimal,2) as anys_antiguitat,
	rm.name as municipi,
	rc.name as comarca,
	rcs.name as provincia,
	rca.name as ccaa
from {{source('erp', 'giscedata_polissa')}} as gp
left join {{source('erp', 'somenergia_soci')}} as ss on ss.partner_id = gp.titular
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


/*
padrins:

SELECT
	CASE
		WHEN personalitat_juridica = 'Persona física' or personalitat_juridica = 'Persona física estrangera' THEN SPLIT_PART(nom_complet, ', ',2)
		ELSE nom_complet
	END as nom,
	CASE
		WHEN personalitat_juridica = 'Persona física' or personalitat_juridica = 'Persona física estrangera' THEN SPLIT_PART(nom_complet, ', ',1)
		ELSE ''
	END
	as cognoms,
	email,
	partner_ref,
	CASE
		WHEN contractes_apadrinats is not null THEN contractes_apadrinats
		ELSE 0
	END as contractes_apadrinats,
	CASE
		WHEN contractes_apadrinats is not null THEN 5 - contractes_apadrinats
		ELSE 5
	END as nebots_disponibles,
	lang as llengua,
	RIGHT(partner_vat, LENGTH(partner_vat) - 2) as dni_nie_nif,
	RIGHT(REPLACE(partner_ref,'O','0'), LENGTH(partner_ref) - 1)::numeric as numero_socia,
	personalitat_juridica
FROM dbt_prod.socies_demografia as socies
left join (
	SELECT soci_padri, count(*) as contractes_apadrinats
	FROM dbt_prod.contractes_socies_demografia as contractes
	WHERE contracte_actiu = TRUE
	GROUP BY soci_padri
	ORDER BY contractes_apadrinats DESC
) as padrins
ON padrins.soci_padri = socies.res_partner_id
where partner_ref NOT IN ('antiga',' ','XXXXS037683','P14869','XXXS021298',' T122937','XXXS026613','XXXS012527','XXXXS025835', 'S011343 no', 'S005996 (No informar del número a la Laura!)')
and (contractes_apadrinats is null or contractes_apadrinats < 5)
ORDER BY padrins.contractes_apadrinats ASC
*/

--# ERRORS
/*
with cagada as (
SELECT
	CASE
		WHEN personalitat_juridica = 'Persona física' or personalitat_juridica = 'Persona física estrangera' THEN SPLIT_PART(nom_complet, ', ',2)
		ELSE nom_complet
	END as nom,
	CASE
		WHEN personalitat_juridica = 'Persona física' or personalitat_juridica = 'Persona física estrangera' THEN SPLIT_PART(nom_complet, ', ',1)
		ELSE ''
	END
	as cognoms,
	email,
	partner_ref,
	CASE
		WHEN contractes_apadrinats is not null THEN contractes_apadrinats
		ELSE 0
	END as contractes_apadrinats,
	CASE
		WHEN contractes_apadrinats is not null THEN 5 - contractes_apadrinats
		ELSE 5
	END as nebots_disponibles,
	lang as llengua,
	RIGHT(partner_vat, LENGTH(partner_vat) - 2) as dni_nie_nif,
	RIGHT(REPLACE(partner_ref,'O','0'), LENGTH(partner_ref) - 1)::numeric as numero_socia,
	personalitat_juridica,
	socia_activa
FROM dbt_prod.socies_demografia as socies
left join (
	SELECT soci_padri, count(*) as contractes_apadrinats
	FROM dbt_prod.contractes_socies_demografia as contractes
	WHERE contracte_actiu = TRUE
	GROUP BY soci_padri
	ORDER BY contractes_apadrinats DESC
) as padrins
ON padrins.soci_padri = socies.res_partner_id
where partner_ref NOT IN ('antiga',' ','XXXXS037683','P14869','XXXS021298',' T122937','XXXS026613','XXXS012527','XXXXS025835', 'S011343 no', 'S005996 (No informar del número a la Laura!)')
and (contractes_apadrinats is null or contractes_apadrinats < 5)
ORDER BY padrins.contractes_apadrinats ASC
), llista_bona as (

SELECT
	CASE
		WHEN personalitat_juridica = 'Persona física' or personalitat_juridica = 'Persona física estrangera' THEN SPLIT_PART(nom_complet, ', ',2)
		ELSE nom_complet
	END as nom,
	CASE
		WHEN personalitat_juridica = 'Persona física' or personalitat_juridica = 'Persona física estrangera' THEN SPLIT_PART(nom_complet, ', ',1)
		ELSE ''
	END
	as cognoms,
	email,
	partner_ref,
	CASE
		WHEN contractes_apadrinats is not null THEN contractes_apadrinats
		ELSE 0
	END as contractes_apadrinats,
	CASE
		WHEN contractes_apadrinats is not null THEN 5 - contractes_apadrinats
		ELSE 5
	END as nebots_disponibles,
	lang as llengua,
	RIGHT(partner_vat, LENGTH(partner_vat) - 2) as dni_nie_nif,
	RIGHT(REPLACE(partner_ref,'O','0'), LENGTH(partner_ref) - 1)::numeric as numero_socia,
	personalitat_juridica,
	socia_activa
FROM dbt_prod.socies_demografia as socies
left join (
	SELECT soci_padri, count(*) as contractes_apadrinats
	FROM dbt_prod.contractes_demografia as contractes
	WHERE contracte_actiu = TRUE
	GROUP BY soci_padri
	ORDER BY contractes_apadrinats DESC
) as padrins
ON padrins.soci_padri = socies.res_partner_id
where partner_ref NOT IN ('antiga',' ','XXXXS037683','P14869','XXXS021298',' T122937','XXXS026613','XXXS012527','XXXXS025835', 'S011343 no', 'S005996 (No informar del número a la Laura!)')
and (contractes_apadrinats is null or contractes_apadrinats < 5)
and socia_activa is true
ORDER BY padrins.contractes_apadrinats ASC
), quantificant as (
	select cagada.partner_ref, cagada.nom, cagada.cognoms, cagada.email, cagada.socia_activa, cagada.contractes_apadrinats as contractes_apadrinats_dolent,
	llista_bona.contractes_apadrinats as contractes_apadrinats_bo, llista_bona.contractes_apadrinats = cagada.contractes_apadrinats as num_apadrinats_check, 
	llista_bona.contractes_apadrinats - cagada.contractes_apadrinats as num_apadrinats_delta
	from cagada as cagada
	full join (select partner_ref, contractes_apadrinats from llista_bona ) as llista_bona
	on cagada.partner_ref = llista_bona.partner_ref
	where socia_activa is true
) --select num_apadrinats_check, count(*) from quantificant group by num_apadrinats_check
select num_apadrinats_delta, count(*) from quantificant group by num_apadrinats_delta

*/


/* SOCIES QUE HAN REBUT QUE TENIEN NEBOTS DISPONIBLES I NO EN TENEN (NO HAURIEN D'HAVER REBUT EL MAIL)
	select * from quantificant where contractes_apadrinats_bo is null
*/

/* QUANTITAT D'ERRORS PER NOMBRE DE NEBOTS DISPONIBLES RESPECTE LA REALITAT
	select num_apadrinats_delta, count(*) from quantificant group by num_apadrinats_delta
*/



/*
with cagada as (
SELECT
	CASE
		WHEN personalitat_juridica = 'Persona física' or personalitat_juridica = 'Persona física estrangera' THEN SPLIT_PART(nom_complet, ', ',2)
		ELSE nom_complet
	END as nom,
	CASE
		WHEN personalitat_juridica = 'Persona física' or personalitat_juridica = 'Persona física estrangera' THEN SPLIT_PART(nom_complet, ', ',1)
		ELSE ''
	END
	as cognoms,
	email,
	partner_ref,
	CASE
		WHEN contractes_apadrinats is not null THEN contractes_apadrinats
		ELSE 0
	END as contractes_apadrinats,
	CASE
		WHEN contractes_apadrinats is not null THEN 5 - contractes_apadrinats
		ELSE 5
	END as nebots_disponibles,
	lang as llengua,
	RIGHT(partner_vat, LENGTH(partner_vat) - 2) as dni_nie_nif,
	RIGHT(REPLACE(partner_ref,'O','0'), LENGTH(partner_ref) - 1)::numeric as numero_socia,
	personalitat_juridica,
	socia_activa
FROM dbt_prod.socies_demografia as socies
left join (
	SELECT soci_padri, count(*) as contractes_apadrinats
	FROM dbt_prod.contractes_socies_demografia as contractes
	WHERE contracte_actiu = TRUE
	GROUP BY soci_padri
	ORDER BY contractes_apadrinats DESC
) as padrins
ON padrins.soci_padri = socies.res_partner_id
where partner_ref NOT IN ('antiga',' ','XXXXS037683','P14869','XXXS021298',' T122937','XXXS026613','XXXS012527','XXXXS025835', 'S011343 no', 'S005996 (No informar del número a la Laura!)')
and (contractes_apadrinats is null or contractes_apadrinats < 5)
ORDER BY padrins.contractes_apadrinats ASC
), llista_bona as (

SELECT
	CASE
		WHEN personalitat_juridica = 'Persona física' or personalitat_juridica = 'Persona física estrangera' THEN SPLIT_PART(nom_complet, ', ',2)
		ELSE nom_complet
	END as nom,
	CASE
		WHEN personalitat_juridica = 'Persona física' or personalitat_juridica = 'Persona física estrangera' THEN SPLIT_PART(nom_complet, ', ',1)
		ELSE ''
	END
	as cognoms,
	email,
	partner_ref,
	CASE
		WHEN contractes_apadrinats is not null THEN contractes_apadrinats
		ELSE 0
	END as contractes_apadrinats,
	CASE
		WHEN contractes_apadrinats is not null THEN 5 - contractes_apadrinats
		ELSE 5
	END as nebots_disponibles,
	lang as llengua,
	RIGHT(partner_vat, LENGTH(partner_vat) - 2) as dni_nie_nif,
	RIGHT(REPLACE(partner_ref,'O','0'), LENGTH(partner_ref) - 1)::numeric as numero_socia,
	personalitat_juridica,
	socia_activa
FROM dbt_prod.socies_demografia as socies
left join (
	SELECT soci_padri, count(*) as contractes_apadrinats
	FROM dbt_prod.contractes_demografia as contractes
	WHERE contracte_actiu = TRUE
	GROUP BY soci_padri
	ORDER BY contractes_apadrinats DESC
) as padrins
ON padrins.soci_padri = socies.res_partner_id
where partner_ref NOT IN ('antiga',' ','XXXXS037683','P14869','XXXS021298',' T122937','XXXS026613','XXXS012527','XXXXS025835', 'S011343 no', 'S005996 (No informar del número a la Laura!)')
and (contractes_apadrinats is null or contractes_apadrinats < 5)
and socia_activa is true
ORDER BY padrins.contractes_apadrinats ASC
), quantificant as (
	select cagada.partner_ref, cagada.nom, cagada.cognoms, cagada.email, cagada.socia_activa, cagada.contractes_apadrinats as contractes_apadrinats_dolent,
	llista_bona.contractes_apadrinats as contractes_apadrinats_bo, llista_bona.contractes_apadrinats = cagada.contractes_apadrinats as num_apadrinats_check, 
	llista_bona.contractes_apadrinats - cagada.contractes_apadrinats as num_apadrinats_delta
	from cagada as cagada
	full join (select partner_ref, contractes_apadrinats from llista_bona ) as llista_bona
	on cagada.partner_ref = llista_bona.partner_ref
	where socia_activa is true
) --select num_apadrinats_check, count(*) from quantificant group by num_apadrinats_check
select * from quantificant where contractes_apadrinats_bo is null
*/
