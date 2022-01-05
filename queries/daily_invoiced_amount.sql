select
	COALESCE(invoices.date_invoice, refounded.date_invoice) date,
	COALESCE(invoices.total,0) as invoiced,
	COALESCE(refounded.total) as refounded,
	COALESCE(invoices.total,0) - COALESCE(refounded.total) as total
from (
	select
		date_invoice,
		sum(amount_total) as total
	from account_invoice as invoice
	where
		invoice.date_invoice >= '2021-11-01' and
		invoice.date_invoice <= '2021-12-31' and
		type = 'out_invoice' and
		true
	group by date_invoice
) as invoices
join
(
	select
		date_invoice,
		sum(amount_total) as total
	from account_invoice as invoice
	where
		invoice.date_invoice >= '2021-11-01' and
		invoice.date_invoice <= '2021-12-31' and
		type = 'out_refund' and
		true
	group by date_invoice
) as refounded
on invoices.date_invoice = refounded.date_invoice
;

