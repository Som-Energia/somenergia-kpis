{{ config(materialized='table') }}

{# This analysis needs many tables of the erp which we do not copy at the moment #}

select cups_input as cups_qb, distri.name as distri
		from giscedata_switching as sw
		left join giscedata_polissa as pol on pol.id = sw.cups_polissa_id
		left join res_partner as distri on distri.id = pol.distribuidora
		where (sw.additional_info like '%-> 42;%' or sw.additional_info like '%-> 43;%')
		and sw.finalitzat is null
		and pol.state = 'activa'
		and sw.proces_id = 3
		and sw.step_id = 22
		and pol.autoconsumo != '43' and pol.autoconsumo != '42'
		group by cups_input, distri.name