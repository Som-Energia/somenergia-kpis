{# taula per trobar la variable que indica que el cas ATR esta obert#}


select id, cups_input, cups_id, cups_polissa_id, accio_pendent,  obrir_cas_pendent, validacio_pendent, finalitzat, actualitzar_tarifa_comer, data_accio
from giscedata_switching
where (additional_info like '%-> 42;%' or additional_info like '%-> 43;%')
and (id = 505543 --esta oberta
or id = 504845 --esta oberta
or id = 504768--esta oberta
or id = 505309 -- esta tancada
or id = 504000 -- esta tancada
or id = 504870) -- esta tancada

{#  finalitzat = null --> oberta 
    finalitzat = true -->tancada/cancelada/esborrany, etc... 
    #}