-- Daily balance of all bank accounts together

select
	item.*,
	sum(item.balance) OVER (
		PARTITION BY true
		ORDER BY item.date
	) as cumsum
from (
	select
		line.date,
		sum(line.debit) as debit,
		sum(line.credit) as credit,
		sum(line.debit-line.credit) as balance
	from account_account as account
	left join account_move_line as line
	on line.account_id = account.id
	where
		--line."date" >= '2021-01-01' and
		code ilike '5720%' and
		code not in (
			'572000000001', -- REMESES AMB VENCIMENT
			'572000000500', -- COMPTE REMESES PONT
			'572000000014', -- PayPal
			'572000000100', -- ARQUIA GL Barcelona
			'572000000101', -- ARQUIA GL Baix Montseny
			'572000000102', -- ARQUIA GL Madrid
			'572000000103', -- ARQUIA GL Lleida
			'572000000104', -- ARQUIA GL Energia Gara
			'572000000105', -- ARQUIA GL Maresme
			'572000000106', -- ARQUIA GL Zaragoza

			'end'
		) and
		true
	group by
		line.date,
		true
	order by
		line.date,
		true
) as item
order by
	item.date

