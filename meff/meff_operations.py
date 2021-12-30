import pandas as pd
import datetime
import urllib.request, gzip

from sqlalchemy import create_engine

from dbconfig import helpscout_api, puppis_prod_db, local_superset_db


# imports
# https://www.meff.es/esp/Derivados-Commodities/Precios-Cierre
# https://www.meff.es/esp/Derivados-Commodities/Precios-Cierre/Excel


def update_closing_prices():

    closing_time = datetime.datetime.now(datetime.timezone.utc)

    print(f'Logging meff closing at {closing_time}')

    precios_cierre = pd.read_html('https://www.meff.es/esp/Derivados-Commodities/Precios-Cierre')[0]
    import pdb; pdb.set_trace()
    precios_cierre.columns = ['dia','res','base_precio','base_dif','base_dif_per','res','punta_precio','punta_dif','punta_dif_per']
    precios_cierre.drop(columns='res', axis=1, inplace=True)
    precios_cierre['fetch_datetime'] = closing_time

    print(precios_cierre)

    engine = create_engine(local_superset_db['dbapi'])

    precios_cierre.to_sql('meff_precios_cierre', engine, index = False, if_exists = 'replace')
    






