-- List all bank accounts

-- Not a metric anybody asked but useful to identify them

select
	account.code,
	account.name
from account_account as account
where
	code ilike '5720%' and
	true
order by
	account.code
