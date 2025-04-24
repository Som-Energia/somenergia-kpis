with spine as (
  select generate_series('2023-01-01', now(), interval '1 month')::date as mes
),
employees as (
	select
	    id,
	    department_id,
	    name,
	    work_email,
	    active,
	    departure_date,
	    first_contract_date
	from {{source('public', 'airbyte_odoo_donyet_hr_employee')}}
),
historical_employees as (
	select
		spine.mes,
		count(*) as num_employees
	from spine
	  left join employees as e
	    on spine.mes between e.first_contract_date and coalesce(e.departure_date, '2100-01-01')
	group by spine.mes
	order by spine.mes
	)

select * from historical_employees