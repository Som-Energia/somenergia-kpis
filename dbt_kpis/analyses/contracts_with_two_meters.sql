-- context https://freescout.somenergia.coop/conversation/8023207?folder_id=108

with polisses_acaparadores as (
	select
		polissa
	from public.giscedata_lectures_comptador
	where data_alta < now() and data_baixa is null
	group by polissa
	having count(distinct name) > 1
)

select
	comptadors.name as comptador_numero,
	comptadors.meter_type as comptador_tipus,
	comptadors.data_alta,
	comptadors.data_baixa,
	p.id as polissa_id,
	p.name as polissa_name,
	p.cups,
	p.distribuidora
from polisses_acaparadores as pa
left join public.giscedata_lectures_comptador as comptadors using (polissa)
left join public.giscedata_polissa p on pa.polissa = p.id
where p.active and p.state = 'activa'
and comptadors.data_alta < now() and comptadors.data_baixa is null
order by distribuidora, polissa_id asc, comptador_tipus