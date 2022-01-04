import pandas as pd
import datetime
import urllib.request, gzip

from sqlalchemy import create_engine

from dbconfig import local_db


# imports
# https://www.meff.es/esp/Derivados-Commodities/Precios-Cierre
# https://www.meff.es/esp/Derivados-Commodities/Precios-Cierre/Excel


def update_neuroenergia(verbose=2, dry_run=False):

    request_time = datetime.datetime.now(datetime.timezone.utc)
    noms_columnes = ['dia','res','base_precio','base_dif','base_dif_per','res','punta_precio','punta_dif','punta_dif_per']

    try:
        precios_cierre = pd.read_html('https://www.meff.es/ing/Commodities-Derivatives/Close-Prices/Excel')[0]
    except:
        # TODO handle exceptions
        raise

    if verbose > 2:
        print(precios_cierre)

    precios_cierre_dia = precios_cierre\
        .set_axis(noms_columnes, axis=1)\
        .drop(columns='res', axis=1)\
        .set_index('dia', drop=False)\
        .filter(regex='^Day', axis=0)\
        .assign(
            request_time = lambda x: request_time,
            dia = lambda x: pd.to_datetime(x['dia'], format='Day %d-%b-%Y').dt.date
        )



    if dry_run:
        print(precios_cierre_dia)
    else:
        engine = create_engine(local_db['dbapi'])
        try:
            precios_cierre_dia.to_sql('meff_precios_cierre_dia', engine, index = False, if_exists = 'append')
        except:
            # TODO handle exceptions
            raise

    return 0
