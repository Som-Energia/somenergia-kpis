--

-- TODO:
-- Prorrateo a un año de energia_kwh i amount_total respecto a days
-- Comprobar si las lecturas estimadas se cuentan bien
-- Usar ids para filtrar journal para acelerar
-- Filtrar facturas por fact.data_inici fact.data_final en vez de invoice.date_invoice (fecha de emision)

select
	contract.cnae,
	contract.partner_vat,
	contract.tariff,
	contract.data_alta,
	contract.data_baixa,
	contract.data_ultima_lectura,
	contract.titular_id = contract.soci_id as titular_soci,
	contract.contracte_telegestionat,
	contract.consum_esperat_data,
 	contract.consum_esperat_kwh,
	MIN(fact.data_inici) AS data_inici,
	MAX(fact.data_final) AS data_final,
	DATE_PART('day', (MAX(data_final)::timestamp - MIN(data_inici)::timestamp)) AS days,
	SUM(case
		when invoice.type='out_refund'
		then -invoice.amount_total
		else invoice.amount_total
	END) as amount_total,
	sum(lectura.n_estimades) as n_estimades,
	sum(invoice.residual) as pendent,
	sum(fact.energia_kwh) as energia_kwh
from (
	select
		pol.id as pol_id,
		pol.name as pol_name,
		pol.data_alta as data_alta,
		pol.data_baixa as data_baixa,
		pol.titular as titular_id,
		pol.soci as soci_id,
		pol.data_ultima_lectura as data_ultima_lectura,
		cups.conany_data as consum_esperat_data,
		cups.conany_kwh as consum_esperat_kwh,
		pol.tg <> '2' as contracte_telegestionat, 
		cnae.name as cnae,
		partner.vat as partner_vat,
		tariff.name as tariff
	from giscedata_polissa_category as cat
	join giscedata_polissa_category_rel as cat_rel
		on cat.id = cat_rel.category_id
	join giscedata_polissa as pol
		on pol.id = cat_rel.polissa_id
	join giscedata_cups_ps as cups
		on cups.id = pol.cups
	join giscemisc_cnae as cnae
		on cnae.id = pol.cnae
	join res_partner as partner
		on partner.id = pol.titular
	join giscedata_polissa_tarifa as tariff
		on tariff.id = pol.tarifa
	where cat.name = 'Entitat o Empresa'
) as contract
join giscedata_facturacio_factura as fact
	on fact.polissa_id = pol_id
join account_invoice as invoice
	on invoice.id = fact.invoice_id
join account_journal as journal
	on journal.id = invoice.journal_id
join (
	SELECT
		count(lectura.origen_id IN (
			--  1, -- Telemesura
			--  2, -- Telemesura corregida
			--  3, -- TPL
			--  4, -- TPL corregida
			  5, -- Visual
			  6, -- Visual corregida
			  7, -- Estimada
			 10, -- Estimada amb l'històric
			 11, -- Estimada amb factor d'utilització
			  8, -- Autolectura
			-- 12, -- Telegestió
			  9,  -- Sense Lectura
			666) OR NULL) as n_estimades,
		factura_id
	FROM giscedata_facturacio_lectures_energia as lectura
	WHERE 
		lectura.magnitud = 'AE' AND
		TRUE
	GROUP BY factura_id
)as lectura
	on lectura.factura_id = fact.id
where
	journal.name = 'Factures Energia' and
	-- invoice.date_invoice between CURRENT_DATE - INTERVAL '13 months'  and CURRENT_DATE - INTERVAL '1 month' and
	invoice.date_invoice between '2020/06/01' and '2021/06/01' and
	true
group by (
	pol_id,
	pol_name,
	data_alta,
	data_baixa,
	titular_id,
	soci_id,
	cnae,
	partner_vat,
	tariff,
	data_ultima_lectura,
	contract.contracte_telegestionat,
	contract.consum_esperat_data,
 	contract.consum_esperat_kwh,
	true
);

