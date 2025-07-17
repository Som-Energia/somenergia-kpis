--@set schema = dbt_dev

set_seed(0.4);

SELECT nom_complet, count(*) as num
    FROM {{ ref('contractes_demografia') }}
    where personalitat_juridica in ('Persona física', 'Persona física estrangera')
    and contracte_actiu
    and nom_complet != 'ANONIMITZAT'
    and email is not null
    and lang is not null
    group by nom_complet
    having count(*) > 1
;

-- seleccio de titulars persona fisica
with titulars as (
	SELECT distinct on(nom_de_pila, nom_complet, email, lang)
	nom_de_pila, nom_complet, email, lang
	FROM {{ ref('contractes_demografia') }}
	where personalitat_juridica in ('Persona física', 'Persona física estrangera')
	and contracte_actiu
	and nom_complet != 'ANONIMITZAT'
	and email is not null
	and lang is not null
)
select * from titulars
order by RANDOM()
limit 100;

select distinct personalitat_juridica
from {{ ref('contractes_demografia') }};
/*
Establiment permanent d'entitat no resident en territori espanyol
Entitat estrangera
Altres tipus no definits en la resta de claus
Persona física
Organisme públic
Associació
Societat de responsabilitat limitada
Corporació Local
Congregació i institució religiosa
Societat civil, amb o sense personalitat jurídica
Desconegut
Societat col·lectiva
Societat cooperativa
Unió Temporal d'Empreses
Comunitat de propietaris en règim de propietat horitzontal
Òrgan de l'Administració de l'Estat i de les Comunitats Autònomes
Persona física estrangera
Societat anònima
Comunitat de béns i herència jacent
Societat comanditària
 */


-- seleccio de 150 eie_socies
with eie_socies as (
	select
		nom_de_pila, nom_complet, email, lang
	from {{ ref('socies_demografia') }}
	where personalitat_juridica in (
		'Societat anònima',
		'Societat de responsabilitat limitada',
		'Societat col·lectiva',
		'Societat comanditària',
		'Comunitat de béns i herència jacent',
		'Societat cooperativa',
		'Associació',
		'Comunitat de propietaris en règim de propietat horitzontal Societat civil, amb o sense personalitat jurídica',
		'Entitat estrangera',
		'Corporació Local',
		'Organisme públic',
		'Congregació i institució religiosa',
		'Òrgan de l''Administració de l''Estat i de les Comunitats Autònomes',
		'Unió Temporal d''Empreses'
	)
	and socia_activa
	and nom_complet != 'ANONIMITZAT'
	and email is not null
	and lang is not null
)
select * from eie_socies
order by RANDOM()
limit 150;

--select * from prod.socies_demografia;

-- socies en general
select genere, count(*) from {{ ref('socies_demografia') }}
where socia_activa
group by genere;


where personalitat_juridica in ('Persona física', 'Persona física estrangera', 'Desconegut');

-- eie_socies
select count(*) from {{ ref('socies_demografia') }}
where personalitat_juridica in (
	'Societat anònima',
	'Societat de responsabilitat limitada',
	'Societat col·lectiva',
	'Societat comanditària',
	'Comunitat de béns i herència jacent',
	'Societat cooperativa',
	'Associació',
	'Comunitat de propietaris en règim de propietat horitzontal Societat civil, amb o sense personalitat jurídica',
	'Entitat estrangera',
	'Corporació Local',
	'Organisme públic',
	'Congregació i institució religiosa',
	'Òrgan de l''Administració de l''Estat i de les Comunitats Autònomes',
	'Unió Temporal d''Empreses'
);

