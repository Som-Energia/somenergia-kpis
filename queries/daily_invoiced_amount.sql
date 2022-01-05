-- Daily amount invoiced

select
	date_invoice as date,
	sum(case
		when type='out_refund' then 0
		else total
	END) as invoiced,
	sum(case
		when type='out_refund' then -total
		else 0
	END) as refound,
	sum(case
		when type='out_refund' then -total
		else total
	END) as total
from (
	select
		date_invoice,
		type,
		sum(amount_total) as total
	from account_invoice as invoice
	where
		--invoice.date_invoice >= '2021-11-01' and
		--invoice.date_invoice <= '2021-12-31' and
		type in ('out_invoice', 'out_refund') and
		true
	group by
		date_invoice,
		type
) as item
group by date_invoice

