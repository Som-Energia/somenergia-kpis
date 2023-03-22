{{ config(materialized='table') }}

{# This analysis needs many tables of the erp which we do not copy at the moment #}

with d102_accepted as (
        select ss.sw_id 
        from giscedata_switching_d1_02 as d102 
        left join giscedata_switching_step_header as ss on ss.id = d102.header_id
        where rebuig = false
    ),
    d102_ac_accepted as (
        select ps.name as cups_d1, ps.id as ps_id, distri.name as distri
        from giscedata_switching_d1_01 as d101
        left join giscedata_switching_step_header as ss on ss.id = d101.header_id
		left join giscedata_switching as sw on sw.id = ss.sw_id
		left join giscedata_cups_ps ps on ps.id = sw.cups_id
        inner join d102_accepted as d102a on ss.sw_id = d102a.sw_id
		left join giscedata_polissa as pol on ps.id = pol.cups
		left join res_partner as distri on distri.id = ps.distribuidora_id
        where motiu_canvi = '04' and collectiu is True and pol.state = 'activa'
    ),
    pol_amb as (
        select ps.name as cups_name
        from giscedata_polissa as p
        left join giscedata_cups_ps as ps on ps.id = p.cups
        where (autoconsumo ilike '42' or autoconsumo ilike '43') 
    ),
	pol_sense_a as (
        select sw.ps_id, cups_d1, distri
        from d102_ac_accepted as sw		
        left join pol_amb as p on sw.cups_d1 = p.cups_name
		where p.cups_name is null 
    ),
	m101_col as (
        select sw.cups_id, m1.id as m1_id, header_id, ss.id as ss_id, ss.sw_id
        from giscedata_switching_M1_01 as m1
    	left join giscedata_switching_step_header as ss on ss.id = m1.header_id
    	left join giscedata_switching as sw on sw.id = ss.sw_id
		where tipus_autoconsum = '42' or tipus_autoconsum = '43'
    ),
	m102_col as (
        select sw.cups_id
        from giscedata_switching_M1_02 as m102
        left join giscedata_switching_step_header as ss on ss.id = m102.header_id
        left join giscedata_switching as sw on ss.sw_id = sw.id
        inner join m101_col as m101 on m101.sw_id = ss.sw_id
		where m102.rebuig = false
    )	 		 
    select cups_d1, distri
	from m102_col 
	inner join pol_sense_a as sa on m102_col.cups_id = sa.ps_id    
	group by cups_d1, distri