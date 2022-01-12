-- Counts the C1 and C2 cases still in draft

-- Processos de canvi de comercialitzadora (C1,C2)
-- al nostre favor i en esborrany, que vol dir
-- que encara no els hem enviat a la distribuidora.

select
	count(crm_case.id)
from crm_case as crm_case
left join giscedata_switching as switching_case
	on switching_case.case_id = crm_case.id
left join giscedata_switching_step as step
	on switching_case.step_id = step.id
left join giscedata_switching_proces as process
	on step.proces_id = process.id
where
	process.name in ('C1', 'C2') and
	step.name = '01' and
	crm_case.state = 'draft' and
	true

