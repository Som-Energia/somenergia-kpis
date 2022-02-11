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
	--sum(invoice.amount_total) as amount_total,
	sum(case
		when invoice.type='out_refund'
		then -amount_total
		else amount_total
	END) as amount_total,
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

