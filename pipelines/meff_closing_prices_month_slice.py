from cmath import log
import sys
import logging

import pandas as pd

import requests

from sqlalchemy import create_engine
import datetime

def extract(engine):

    meff_closing_prices = pd.read_sql_table('meff_closing_prices', engine)

    return meff_closing_prices

def transform(closing_prices: pd.DataFrame):

    create_time = datetime.datetime.now(datetime.timezone.utc)

    closing_prices_transformed = closing_prices\
        .set_index('price_date', drop=False)\
        .filter(regex='^Month', axis=0)\
        .assign(
            price_date = lambda x: pd.to_datetime(x['price_date'], format='Month %b-%Y').dt.date,
            emission_date = lambda x: pd.to_datetime(x['emission_date'], format='%A, %d %B, %Y').dt.date,
        )\
        .reset_index(drop=True)

    closing_prices_transformed_latest = closing_prices_transformed\
        .sort_values('create_time', ascending = False)\
        .drop_duplicates(['emission_date','price_date'])\
        .sort_values('price_date')\
        .reset_index(drop=True)\
        .assign(create_time = create_time)

    return closing_prices_transformed_latest

def load(engine, closing_prices_shaped: pd.DataFrame, dry_run=False):

    if dry_run:
        return logging.info(closing_prices_shaped)

    closing_prices_shaped.to_sql('meff_closing_prices_month', engine, index = False, if_exists = 'replace')

def slice_closing_prices(dbapi, dry_run=False):

    engine = create_engine(dbapi)

    meff_closing_prices = extract(engine)
    meff_closing_prices = transform(meff_closing_prices)
    load(engine, meff_closing_prices,dry_run)

    return meff_closing_prices

if __name__ == '__main__':
    dbapi = sys.argv[1]
    dry_run = sys.argv[2] if len(sys.argv) > 2 else False
    slice_closing_prices(dbapi, dry_run)