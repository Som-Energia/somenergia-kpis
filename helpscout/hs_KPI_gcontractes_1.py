from helpscout.client import HelpScout
import pandas as pd
from sqlalchemy import create_engine
import datetime
from datetime import date
from datetime import timedelta
import getopt, sys
import click

from dbconfig import helpscout_api, puppis_prod_db

# Mailboxes objectiu:
# {'id': 25853, 'name': 'Comercialització Som Energia', 'slug': '0d45beceef2334dc', 'email': 'comercialitzacio@somenergia.coop'}
# {'id': 25863, 'name': 'Modifica Som Energia', 'slug': 'e143711a22f6c0bf', 'email': 'modifica@somenergia.coop'}
# KPI:
#   Núm de correus respostos a les bústies Comer i Modifica.
#   suma total i també per etiqueta del HS."
# Targeta:
# https://trello.com/c/xCsZRcXR/4595-0-6-p28-an%C3%A0lisi-afegir-kpis-de-helpscout-al-superset

def create_HS_engine():
    engine = create_engine(puppis_prod_db['dbapi'])
    hs = HelpScout(app_id=helpscout_api['app_id'], app_secret=helpscout_api['app_secret'], sleep_on_rate_limit_exceeded=True)
    return hs, engine


def set_params(inici, fi):
    """
    Auto:
        Fem una crida a la API de HelpScout per les bústies de 'Comercialització Som Energia' i 'Modifica Som Energia'
        Només correus tancats i de l'última setmana. Pensat per ser executat cada dilluns.
    Manual:
        El mateix però triem l'interval de dades
    """
    if None in (inici, fi):
        lastSunday = date.today() - timedelta(days=(date.today().weekday() - 6) % 7)
        lastMonday = lastSunday - timedelta(days=(lastSunday.weekday() - 0) % 7)
        lastSunday = lastSunday.strftime("%Y-%m-%dT23:59:59Z")
        lastMonday = lastMonday.strftime("%Y-%m-%dT00:00:00Z")
        params = 'query=(modifiedAt:['+lastMonday+' TO '+lastSunday+'])&mailbox=25853&mailbox=25863&status=closed' 
        return params
    else:
        dataInici = inici + "T00:00:00Z"
        dataFi = fi + "T23:59:59Z"
        params = 'query=(modifiedAt:['+dataInici+' TO '+dataFi+'])&mailbox=25853&mailbox=25863&status=closed' 
        return params

def get_conversations(params, hs, engine):
    """
    només som capaços de filtrar dates amb modifiedAt i createdAt. Ni close, ni res més sembla que tingui paràmetre propi a 
    la API. Data modificat és = a data de close pq és l'última acció, suposem:
    """
    
    conversations = hs.conversations.get(params=params)

    convtags = pd.DataFrame(
        [[
            c.id,
            c.createdAt,
            c.closedAt,
            c.mailboxId,
            [x['tag'] for x in c.tags],
            c.status
        ] for c in conversations],
        columns=['id_conv','hs_created_at','hs_closed_at','mailbox_id','tag','status']
    )

    convtags = convtags.explode('tag')

    convtags['hs_created_at'] = pd.to_datetime(convtags['hs_created_at'])
    convtags['hs_closed_at'] = pd.to_datetime(convtags['hs_closed_at'])
    convtags['inserted_at'] = datetime.datetime.now()
    convtags.to_sql('hs_gcontractes_tags_test', engine, index = False, if_exists = 'append')

@click.command()
@click.option('--inici', default=None, help='Inici del períde a capturar')
@click.option('--fi', default=None, help='Inici del períde a capturar')
def main(inici, fi):    
    hs, engine = create_HS_engine()
    params = set_params(inici, fi)
    get_conversations(params, hs, engine)

if __name__ == '__main__':
    main()


"""
    KPI GContractes 1:

select 
	tag,
	week,
	avg(response_time_hours) as response_time_hours_avg,
	min("hs_closed_at") as hs_closed_at_min,
	max("hs_closed_at") as hs_closed_at_max,
	count(tag) as n
from (
	select 
		tag,
		EXTRACT(epoch FROM "hs_closed_at" - "hs_created_at")/3600 as response_time_hours,
		date_part('week',"hs_closed_at") as week,
		"hs_closed_at"
	from hs_gcontractes_tags as hgt
	where 
		EXTRACT(epoch FROM "hs_closed_at" - "hs_created_at")/3600 > 0 and
		EXTRACT(epoch FROM "hs_closed_at" - "hs_created_at")/3600 < 1440 and
		"mailbox_id" = '25853' and
		tag IN ('baixa b1','altes subministrament','urgent','c6 baixa','comprovació adreça','ssaa')
	order by week desc
) as convs_tags
group by tag, week

"""