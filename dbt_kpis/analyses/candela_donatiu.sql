
{{ config(enabled=False) }}

{# needs some models we currently do not sync. Use it against an erp database #}

with donatius_candela as (
select
    p.name as polissa,
    cups.name as cups,
    date_trunc('month', i.date_invoice)::date as mes,
    count(f.id) as num_factures,
    coalesce(sum(il.price_subtotal) filter (where i.type = 'out_refund'), 0) as refunds,
	coalesce(sum(il.price_subtotal) filter (where i.type = 'out_invoice'), 0) as donatiu_voluntari,
	sum(f.energia_kwh) filter (where i.type = 'out_invoice') as energia_kwh,
	coalesce(sum(f.energia_kwh) filter (where i.type = 'out_refund'), 0) as energia_kwh_refund
from
    giscedata_polissa as p
    left join giscedata_facturacio_factura as f on f.polissa_id = p.id
    left join account_invoice as i on f.invoice_id = i.id
    left join account_invoice_line as il on il.invoice_id = i.id
	left join giscedata_cups_ps as cups on cups.id = p.cups
where
    soci = 190794  and
    i.type IN ('out_refund', 'out_invoice') and
    i.state = 'paid' and
    f.data_inici >= '2021-01-01' and
    il.product_id in (213, 425, 211) -- el 'producte' donatiu ha canviat d'identificador en el temps
group by p.name, cups.name, mes
order by mes asc, p.name desc
)
select
	date_trunc('year', mes) as year,
	count(distinct polissa) as num_cups_still_donating_currently,
	sum(num_factures) as num_factures,
	sum(donatiu_voluntari) as donatiu_voluntari,
	sum(refunds) as refunds,
	sum(energia_kwh) - sum(energia_kwh_refund) as energia_kwh
from donatius_candela
group by year