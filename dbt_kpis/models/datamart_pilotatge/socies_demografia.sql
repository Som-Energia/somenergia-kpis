{{ config(materialized='view') }}
 select
 	partner_ref,
	res_partner_id,
	nom_de_pila,
	nom_complet,
	partner_vat,
	personalitat_juridica,
	phone,
	mobile,
	email,
	lang,
	partner_municipi as municipi,
	partner_comarca as comarca,
	partner_provincia as provincia,
	partner_ccaa as ccaa,
	genere,
	max(potencia) as potencia_maxima_contractada,
	case WHEN max(autoconsumo::integer) > 0 THEN TRUE ELSE FALSE END as te_autoconsum,
	count(*) as contractes,
	SUM(contracte_actiu::integer) as contractes_actius,
	case WHEN SUM(socia_activa::integer) > 0 THEN TRUE ELSE FALSE END as socia_activa,
	MAX(anys_antiguitat) as anys_antiguitat
from {{ref('contractes_socies_demografia')}}
--where res_partner_id is not null
group by
	partner_ref,
	res_partner_id,
	nom_de_pila,
	nom_complet,
	partner_vat,
	personalitat_juridica,
	phone,
	mobile,
	email,
	lang,
	partner_municipi,
	partner_comarca,
	partner_provincia,
	partner_ccaa,
	genere

/*
SELECT setseed(0.5)
SELECT *
FROM dbt_prod.socies_demografia
WHERE socia_activa IS TRUE
ORDER BY RANDOM() LIMIT 3000

treballadores que són socies
with dnis as (
	SELECT *
	FROM (
		VALUES ('ES46746011Y'),('ES40451779T'),('ES40316473A'),('ES39375473W'),('ES047755958T'),('ES46736723X'),('ES47875106P'),('ES77911458Q'),('ES40357564Q'),('ES46766179A'),('ES40323487W'),('ES36517097C'),('ES43685680D'),('ES35563735P'),('ES77910266C'),('ES53125892D'),('ES07264264J'),('ES45679960M'),('ES077921500F'),('ES52214831R'),('ES37375939L'),('ES43698506R'),('ES40356252S'),('ES43630451A'),('ES47732469V'),('ES38830071E'),('ES38844955W'),('ES47109770C'),('ES41507325F'),('ES79350053F'),('ES40360569P'),('ES53502366L'),('ES44834143J'),('ES77922283P'),('ES41550823N'),('ES46148580T'),('ES47721824K'),('ES45463586S'),('ES46120347B'),('ES44325811G'),('ES47822261V'),('ES41549516Q'),('ES46717062Z'),('ES47865179V'),('ESY2140131H'),('ES41589924J'),('ES47771348A'),('ES41634491Y'),('ES53294554N'),('ES40357426Q'),('ES47784482G'),('ES39373115J'),('ES39380113L'),('ES77317871J'),('ES40327754Z'),('ES36983667N'),('ES37292336K'),('ES40935052C'),('ES39389749H'),('ES41559326M'),('ES42327583Q'),('ES40362071S'),('ES47270168Q'),('ES40357479T'),('ES40345613W'),('ES47623531F'),('ES44980754E'),('ES37330391B'),('ES41670640E'),('ES47810735Z'),('ESY5225263E'),('ES79295419K'),('ES45497247G'),('ES47277669L'),('ES77616573Z'),('ES40334096P'),('ES43549676G'),('ES26758251M'),('ES36516413A'),('ES40520162G'),('ES45545162X'),('ES38871719V'),('ES47813258F'),('ES41698954T'),('ES39924153V'),('ESY1394928S'),('ES41668413A'),('ES47918337E'),('ES40335700W'),('ES52173689Y'),('ES40352165E'),('ES46220968F'),('ES41577406F'),('ES46644176S'),('ES41577461Q'),('ES41556036G'),('ES45962968K'),('ES46757910Z'),('ES43633587B'),('ES38873041M'),('ES45785985T'),('ES38864109C'),('ES77302536L'),('ES47871996A'),('ES45643997Z'),('ES41558234V'),('ES01935019Y'),('ES47271147Y'),('ES54130726L'),('ES41628113E'),('ES47176145V'),('ES46692697Y'),('ES47918932L'),('ES47970434R'),('ES46739355C'),('ES46226180K'),('ES46816626B'),('ES20840153Z'),('ES41594047L'),('ES47719531M'),('ES40343332K'),('ES47151390X'),('ES47855160A'),('ES45789556Y'),('ESY7018957S'),('ES41568393X'),('ES77920169X'),('ES41592944C'),('ES47830019R'),('ES41559810Y'),('ESY0354506K'),('ES41550069V'),('ES38868026G'),('ES45125210S'),('ES28921667H'),('ES48824971L'),('ES41577430P'),('ES47771078D')
	) AS t(partner_vat)
)

select *
from dnis as treballadores
left join (select * from dbt_prod.socies_demografia where socia_activa = true) as socies
on treballadores.partner_vat = socies.partner_vat

*/