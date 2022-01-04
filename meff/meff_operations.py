import pandas as pd
import datetime
import urllib.request, gzip

from sqlalchemy import create_engine

from dbconfig import local_db


# imports
# https://www.meff.es/esp/Derivados-Commodities/Precios-Cierre
# https://www.meff.es/esp/Derivados-Commodities/Precios-Cierre/Excel


def update_closing_prices_day(verbose=2, dry_run=False):

    request_time = datetime.datetime.now(datetime.timezone.utc)
    noms_columnes = ['dia','res','base_precio','base_dif','base_dif_per','res','punta_precio','punta_dif','punta_dif_per']
    if not dry_run:
        engine = create_engine(local_db['dbapi'])

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
        try:
            precios_cierre_dia.to_sql('meff_precios_cierre_dia', engine, index = False, if_exists = 'append')
        except:
            # TODO handle exceptions
            raise

    return 0

def update_closing_prices_month(verbose=2, dry_run=False):

    request_time = datetime.datetime.now(datetime.timezone.utc)
    noms_columnes = ['mes','res','base_precio','base_dif','base_dif_per','res','punta_precio','punta_dif','punta_dif_per']
    if not dry_run:
        engine = create_engine(local_db['dbapi'])

    try:
        precios_cierre = pd.read_html('https://www.meff.es/ing/Commodities-Derivatives/Close-Prices/Excel')[0]
    except:
        # TODO handle exceptions
        raise

    if verbose > 2:
        print(precios_cierre)

    precios_cierre_mes = precios_cierre\
        .set_axis(noms_columnes, axis=1)\
        .drop(columns='res', axis=1)\
        .set_index('mes', drop=False)\
        .filter(regex='^Month', axis=0)\
        .assign(
            request_time = lambda x: request_time,
            dia = lambda x: pd.to_datetime(x['mes'], format='Month %b-%Y').dt.date
        )

    if dry_run:
        print(precios_cierre_mes)
    else:
        try:
            precios_cierre_mes.to_sql('meff_precios_cierre_mes', engine, index = False, if_exists = 'append')
        except:
            # TODO handle exceptions
            raise

    return 0


def update_las_closing_prices_long(verbose=2, dry_run=False):
    # To Do: Fitxer amb historic + previsio?
    # https://www.meff.es/esp/Derivados-Commodities/Ultimos-Precios-Cierre
    pass
