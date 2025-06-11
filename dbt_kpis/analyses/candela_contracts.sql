select
	count(p.id),
	count(*) filter (where p.active and p.data_baixa is null and p.state = 'activa') as contracte_actiu,
    count(*) filter (where donatiu) as with_donatiu
from
    {{ source('erp', 'giscedata_polissa') }} as p
    where soci = 190794