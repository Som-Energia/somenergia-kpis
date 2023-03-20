import pandas as pd
from erppeek import Client
from dbconfig import erppeek

client = Client(**erppeek)                                                             

Switching = client.model('giscedata.switching') 


# 1- Nº de contractes amb una M cap a auto 42 i 43, en estat tancat, 
# i que en el contracte no aparegui autoconsum actiu.


# via erpclient: 
#   
# cups_iniciats = cerquem tots els cups que en algun moment han fet un tramit d'auto col.lectiu: 
#       tots els casos >    'Informació adicional': '-> 42;'  i  'Proces': M
#       tots els casos >    'Informació adicional': '-> 43;'  i  'Proces': M
# cups_auto_col = cercar a totes les polisses: auto 42 i a totes les polisses: auto 43
# cups_sense = cups_iniciats - cups_auto_col

# a erppeek seria algo tipus
# ids = Switching.search([('additional_info','ilike','%-> 42;%'),('state','ilike','closed')])

    
# cups_polissa_id --> id de la polissa

# no recomana fer servir polissa_id
# ni la de sota


# ___________________________________________________________________________________
# Nº de contractes amb D101(04) col·lectiu en pas 02 d'acceptació, 
# sense M102 obertes i acceptades i que no tinguin autoconsum actiu en el contracte



# distri --d101--> nosaltres : parlem amb la sòcia
# nosaltres --d102-> distri : acceptació
 


# que hi hagi un M1 i no hagi arribat l'M2 o posterior



# giscedata_switching_d1_01.collectiu = true

# sw_d101 = llista de tots els casos de switching d101 amb motiu de canvi 04 i collectiu true


# llista de casos de switching d101 i busquem els que tinguin el d102 amb rebuig = false

# sw_d102_acceptats = la llista que ens queda totes les peticions d'autoconsum collectiu de les que hem iniciat el tràmit

sw_d102_acceptat = '''
with d1_02_rejects as (
select d102.*, ss.id, ss.sw_id 
from giscedata_switching_d1_02 as d102 
left join giscedata_switching_step_header as ss on ss.id = d102.header_id
where rebuig = false
)

select
    *
from giscedata_switching_d1_01 as d101
left join giscedata_switching_step_header as ss on ss.id = d101.header_id
inner join d1_02_rejects as d102r on ss.sw_id = d102r.sw_id
where motiu_canvi = '04' and collectiu is True
'''
# pol_amb = cerca de totes les polisses amb auto collectiu -- serveix per a treure les polisses que la distri no s'ha esperat a tenir l'm1 al punt següent

pol_amb_a = '''
select *
from giscedata_polissa
where autoconsumo ilike '43' or autoconsumo ilike '42'
'''

# pol_sense = sw_d102_acceptats - pol_amb polisses que han intentat autoconsum collectiu i no ho han aconseguit (encara)

pol_sense_a = '''

with sw_d102_acceptat as (
    [..]
),
pol_amb as (
    [..]
)

select *
from sw_d102_acceptat as sw
left join pol_amb as p on sw.cups ilike p.cups
where p.cups is null
'''



# m1_pol_sense_a = totes les m1 relacionades amb canvi a 42 o 43, de les pol_sense 

m101_col = '''
select *
from giscedata_switching_M1_01
where tipus_autoconsum = '42' or tipus_autoconsum = '43'
'''

# m1_pol_sense_a = pol_sense_a - m101_col

m1_pol_sense_a = '''
with d1_02_accepted as (
    select d102.*, ss.id, ss.sw_id 
    from giscedata_switching_d1_02 as d102 
    left join giscedata_switching_step_header as ss on ss.id = d102.header_id
    where rebuig = false
),
sw_d102_acceptat as (
    select *
    from giscedata_switching_d1_01 as d101
    left join giscedata_switching_step_header as ss on ss.id = d101.header_id
    inner join d1_02_accepted as d102a on ss.sw_id = d102a.sw_id
    where motiu_canvi = '04' and collectiu is True
),
pol_amb as (
    select *, ps.name as cups_name, ps.id as ps_id
    from giscedata_polissa as p
    left join giscedata_cups_ps as ps on ps.id = p.cups
    where autoconsumo ilike '43' or autoconsumo ilike '42' 
),
pol_sense_a as (
    select *, sw.cups as sw_cups
    from sw_d102_acceptat as sw
    left join pol_amb as p on sw.cups = p.cups_name
    where p.cups_name is null
),
m101_col as (
    select *
    from giscedata_switching_M1_01
    where tipus_autoconsum = '42' or tipus_autoconsum = '43'
)
select *
from pol_sense_a as sa
left join giscedata_switching as sw on sw.cups_id = sa.ps_id
left join giscedata_switching_step_header as ss on ss.sw_id = sw.id
left join m101_col as m1 on m1.header_id = ss.id
where m1.id is null
'''
#no hi ha cap M101 sense polissa_auto_col

# m2_pol_sense = totes les m2 relacionades amb canvi a 42 o 43, de les pol_sense 

# m5_pol_sense = totes les m5 relacionades amb canvi a 42 o 43, de les pol_sense

# pol_sense_amb_m = m1_pol_sense - m2_pol_sense - m5_pol_sense : polisses que no tinguin m2 o posterior









