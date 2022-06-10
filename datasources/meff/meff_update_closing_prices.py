import sys
import logging

import pandas as pd

import datetime
import urllib.request, gzip
import lxml.html
import requests

from sqlalchemy import create_engine


# imports
# https://www.meff.es/esp/Derivados-Commodities/Precios-Cierre
# https://www.meff.es/esp/Derivados-Commodities/Precios-Cierre/Excel


def get_prices_from_url():
    try:
        url = 'https://www.meff.es/ing/Commodities-Derivatives/Close-Prices/Excel'
        precios_cierre = pd.read_html(url)[0]
        emission_date = lxml.html.fromstring(requests.get(url).text).xpath('//*[@class="tblcaption peq"]')[0].text.strip()
    except:
        # TODO handle exceptions
        raise
    return precios_cierre, emission_date

def shape_closing_prices(closing_prices: pd.DataFrame, emission_date):

    request_time = datetime.datetime.now(datetime.timezone.utc)
    noms_columnes = ['price_date','res','base_precio','base_dif','base_dif_per','res','punta_precio','punta_dif','punta_dif_per']

    closing_prices_shaped = closing_prices\
        .set_axis(noms_columnes, axis=1)\
        .assign(emission_date=emission_date, create_time=request_time)\
        .drop(columns='res', axis=1)

    return closing_prices_shaped

def save_to_db(engine, closing_prices_shaped: pd.DataFrame):
    closing_prices_shaped.to_sql('meff_closing_prices', engine, index = False, if_exists = 'append')

def update_closing_prices(dbapi, dry_run=False):

    # TODO support dry running
    dry_run = False

    engine = create_engine(dbapi)
    closing_prices_raw, emission_date = get_prices_from_url()
    closing_prices = shape_closing_prices(closing_prices_raw, emission_date)

    if dry_run:
        logging.info(closing_prices)
    else:
        save_to_db(engine, closing_prices)

    return closing_prices

if __name__ == '__main__':
    dbapi = sys.argv[1]
    dry_run = sys.argv[2] if len(sys.argv) > 2 else False
    update_closing_prices(dbapi, dry_run)