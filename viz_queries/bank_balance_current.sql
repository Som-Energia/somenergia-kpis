-- Current balance for each bank separatelly

select
	distinct on (item.code)
	item.date,
	item.code,
	item.name,
	sum(item.balance) OVER (
		PARTITION BY item.code
		ORDER BY item.date
	) as cumsum
from (
	select
		line.date,
		account.code,
		account.name,
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
		account.code,
		account.name,
		true
	order by
		account.code,
		line.date,
		true
) as item
order by
	item.code,
	item.date desc



