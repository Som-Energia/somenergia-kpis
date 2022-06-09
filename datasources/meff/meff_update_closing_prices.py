import sys
import logging

import pandas as pd

import datetime
import urllib.request, gzip

from sqlalchemy import create_engine


# imports
# https://www.meff.es/esp/Derivados-Commodities/Precios-Cierre
# https://www.meff.es/esp/Derivados-Commodities/Precios-Cierre/Excel


def get_excel_prices_from_url():
    try:
        precios_cierre = pd.read_html('https://www.meff.es/ing/Commodities-Derivatives/Close-Prices/Excel')[0]
    except:
        # TODO handle exceptions
        raise

def shape_closing_prices(closing_prices: pd.DataFrame):

    request_time = datetime.datetime.now(datetime.timezone.utc)
    noms_columnes = ['dia','res','base_precio','base_dif','base_dif_per','res','punta_precio','punta_dif','punta_dif_per']

    closing_day_prices = closing_prices\
        .set_axis(noms_columnes, axis=1)\
        .drop(columns='res', axis=1)\
        .set_index('dia', drop=False)\
        .filter(regex='^Day', axis=0)\
        .assign(
            request_time = lambda x: request_time,
            dia = lambda x: pd.to_datetime(x['dia'], format='Day %d-%b-%Y').dt.date
        )

    return closing_day_prices

def save_to_db(engine, closing_day_prices: pd.DataFrame):
    closing_day_prices.to_sql('meff_precios_cierre_dia', engine, index = False, if_exists = 'append')

def update_closing_prices(dbapi, dry_run=False):

    # TODO support dry running
    dry_run = False

    engine = create_engine(dbapi)
    closing_day_prices_raw = get_excel_prices_from_url()
    closing_day_prices = shape_closing_prices(closing_day_prices_raw)

    if dry_run:
        logging.info(closing_day_prices)
    else:
        save_to_db(engine, closing_day_prices)

    return closing_day_prices

if __name__ == '__main__':
    dbapi = sys.argv[1]
    dry_run = sys.argv[2] if sys.argc > 1 else False
    update_closing_prices(dbapi, dry_run)