{{ config(materialized='table') }}

{# This analysis needs many tables of the erp which we do not copy at the moment #}

with pol_amb_alguna_M_col as (
		select cups_input as pol_amb_alguna_M_col, distri.name as distri
		from giscedata_switching as sw
		left join giscedata_cups_ps ps on ps.id = sw.cups_id
		left join res_partner as distri on distri.id = ps.distribuidora_id
		where (additional_info like '%-> 42;%' or additional_info like '%-> 43;%')
		group by cups_input, distri.name
	),
	pol_col_actives as (
        select ps.name as auto_col_actiu
		from giscedata_polissa as p
        left join giscedata_cups_ps as ps on ps.id = p.cups
        where (autoconsumo ilike '42' or autoconsumo ilike '43') 
		and state = 'activa'
	), 
	d102_accepted as (
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
    pol_col_totes as (
        select ps.name as pol_col
        from giscedata_polissa as p
        left join giscedata_cups_ps as ps on ps.id = p.cups
        where (autoconsumo ilike '42' or autoconsumo ilike '43') 
    ),
	 pol_col_baixa as (
        select ps.name as pol_col_baixa
        from giscedata_polissa as p
        left join giscedata_cups_ps as ps on ps.id = p.cups
        where (autoconsumo ilike '42' or autoconsumo ilike '43') 
		and state != 'activa'
	),
	pol_sense_col as (
        select sw.ps_id, cups_d1, distri
        from d102_ac_accepted as sw		
        left join pol_col_totes as p on sw.cups_d1 = p.pol_col
		where p.pol_col is null 
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
    ), 
	pol_tramit_actiu as (
		select cups_d1 as tramit_actiu
		from m102_col 
		inner join pol_sense_col as sa on m102_col.cups_id = sa.ps_id    
		group by cups_d1
	),
	pol_1M_tancada as (	
		select cups_input as cups_alguna_M_rebuig
		from giscedata_switching as sw
		left join giscedata_polissa as pol on pol.id = sw.cups_polissa_id
		left join res_partner as distri on distri.id = pol.distribuidora
		where (sw.additional_info like '%-> 42;%' or sw.additional_info like '%-> 43;%')
		and sw.finalitzat is not null
		and pol.state = 'activa'
		and sw.proces_id = 3
		and pol.autoconsumo != '43' and pol.autoconsumo != '42'
		group by cups_input, distri.name
	),
	tramit_aturat as (
		select mt.cups_alguna_M_rebuig as tramit_aturat
		from pol_1m_tancada as mt
		left join pol_tramit_actiu as ta on mt.cups_alguna_M_rebuig = ta.tramit_actiu
		where ta.tramit_actiu is null
	),
	pol_col_tramitasom_baixa as (
		select cups_input as auto_col_i_baixa
		from giscedata_switching as sw
		left join giscedata_polissa as pol on pol.id = sw.cups_polissa_id
		left join res_partner as distri on distri.id = pol.distribuidora
		where (sw.additional_info like '%-> 42;%' or sw.additional_info like '%-> 43;%')
		and pol.state = 'baixa'
		and (pol.autoconsumo ilike '42' or pol.autoconsumo ilike '43')
		group by cups_input
	),
	baixa_abans_col as (
		select cups_input as baixa_abans_dauto_col
		from giscedata_switching as sw
		left join giscedata_polissa as pol on pol.id = sw.cups_polissa_id
		where (additional_info like '%-> 42;%' or additional_info like '%-> 43;%')
		and pol.autoconsumo != '43' and pol.autoconsumo != '42'
		and pol.state = 'baixa'
		group by cups_input 
	)
	
	select 
	*,
	case 
		when baixa_abans_dauto_col is not null then 'baixa_abans_dauto_col'
	  when auto_col_actiu is not null then 'auto_col_actiu'
	  when auto_col_i_baixa is not null then 'auto_col_i_baixa'
	  when tramit_aturat is not null then 'tramit_aturat'
	  when tramit_actiu is not null then 'tramit_actiu'
	  else 'altres'
	end as tramit_status
	from pol_amb_alguna_M_col as ini
	left join pol_col_actives as pa on pa.auto_col_actiu = ini.pol_amb_alguna_M_col
	left join tramit_aturat as ta on ta.tramit_aturat = ini.pol_amb_alguna_M_col
	left join pol_tramit_actiu as tac on tac.tramit_actiu = ini.pol_amb_alguna_M_col
	left join pol_col_tramitasom_baixa as b on b.auto_col_i_baixa = ini.pol_amb_alguna_M_col
	left join baixa_abans_col as ib on ib.baixa_abans_dauto_col = ini.pol_amb_alguna_M_col