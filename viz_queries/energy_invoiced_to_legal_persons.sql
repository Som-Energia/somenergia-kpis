-- Find the last year consumption and invoiced amount for
-- contracts owned by legal entities.

-- TODO:
-- [ ] Comprobar si las lecturas estimadas se cuentan bien



SELECT
	contract.pol_name AS contracte,
	contract.cnae,
	contract.partner_vat AS nif_titular,
	contract.tariff,
	contract.data_alta,
	contract.data_baixa,
	contract.data_ultima_lectura,
	contract.titular_id = contract.soci_id AS titular_es_soci,
	contract.contracte_telegestionat,
	MIN(fact.data_inici) AS primera_data_considerada,
	MAX(fact.data_final) AS darrera_data_considerada,
	DATE_PART('day', (MAX(data_final)::timestamp - MIN(data_inici)::timestamp)) AS days,
	ROUND(SUM(cAS
		when invoice.type='out_refund'
		then -invoice.amount_total
		else invoice.amount_total
	END) * 365 / NULLIF(DATE_PART('day', (MAX(data_final)::timestamp - MIN(data_inici)::timestamp)),0)) AS facturacio_proratejada_any,
	sum(lectura.n_estimades)>0 AS te_lectures_estimades,
	sum(invoice.residual) AS pagaments_pendents,
	ROUND(sum(fact.energia_kwh) * 365 / NULLIF(DATE_PART('day', (MAX(data_final)::timestamp - MIN(data_inici)::timestamp)),0)) AS energia_kwh_prorratejada_any,
 	contract.consum_esperat_kwh AS estimacio_gisce_kwhany,
	contract.consum_esperat_data AS estimacio_gisce_data
FROM (
	SELECT
		pol.id AS pol_id,
		pol.name AS pol_name,
		pol.data_alta AS data_alta,
		pol.data_baixa AS data_baixa,
		pol.titular AS titular_id,
		pol.soci AS soci_id,
		pol.data_ultima_lectura AS data_ultima_lectura,
		cups.conany_data AS consum_esperat_data,
		cups.conany_kwh AS consum_esperat_kwh,
		pol.tg <> '2' AS contracte_telegestionat, 
		cnae.name AS cnae,
		partner.vat AS partner_vat,
		tariff.name AS tariff
	from giscedata_polissa AS pol
	--join giscedata_polissa_category_rel AS cat_rel
	--	on pol.id = cat_rel.polissa_id
	join giscedata_cups_ps AS cups
		on cups.id = pol.cups
	join giscemisc_cnae AS cnae
		on cnae.id = pol.cnae
	join res_partner AS partner
		on partner.id = pol.titular
	join giscedata_polissa_tarifa AS tariff
		on tariff.id = pol.tarifa
	where TRUE
	--and cat_rel.category_id = 33 -- cat.name = 'Entitat o Empresa'
	and pol.data_baixa IS NULL
	and tariff.pot_max > 15
) AS contract
join giscedata_facturacio_factura as fact
	on fact.polissa_id = pol_id
join account_invoice as invoice
	on invoice.id = fact.invoice_id
join (
	SELECT
		count(lectura.origen_id IN (
			--  1, -- Telemesura
			--  2, -- Telemesura corregida
			--  3, -- TPL
			--  4, -- TPL corregida
			--  5, -- Visual
			--  6, -- Visual corregida
			  7, -- Estimada
			 10, -- Estimada amb l'històric
			 11, -- Estimada amb factor d'utilització
			--  8, -- Autolectura
			-- 12, -- Telegestió
			  9,  -- Sense Lectura
			666) OR NULL) as n_estimades,
		factura_id
	FROM giscedata_facturacio_lectures_energia as lectura
	WHERE TRUE
	AND lectura.magnitud = 'AE'
	GROUP BY factura_id
) AS lectura
	ON lectura.factura_id = fact.id
WHERE TRUE
	AND invoice.journal_id = 5 -- journal.name = 'Factures Energia' and
	AND fact.data_final::date > contract.data_ultima_lectura::date - INTERVAL '12 months'
GROUP BY (
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
)
ORDER BY
	pol_name
;

