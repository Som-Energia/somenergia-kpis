{{ config(materialized='view') }}
 select
	res_partner_id,
	nom_de_pila,
	nom_complet,
	phone,
	mobile,
	email,
	lang,
	max(potencia) as potencia_maxima_contractada,
	case WHEN max(autoconsumo::integer) > 0 THEN TRUE ELSE FALSE END as te_autoconsum,
	SUM(contracte_actiu::integer) as contractes_actius,
	case WHEN SUM(socia_activa::integer) > 0 THEN TRUE ELSE FALSE END as socia_activa,
	MAX(edat_som) as edat_primer_contracte
from {{ref('contractes_socies_demografia')}}
--where res_partner_id is not null
group by
	res_partner_id,
	nom_de_pila,
	nom_complet,
	phone,
	mobile,
	email,
	lang