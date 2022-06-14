import sys
import logging

import pandas as pd

import datetime
import urllib.request, gzip
import lxml.html
import requests

from sqlalchemy import create_engine

def get(url):
    try:
        df_raw = pd.read_csv(url)
    except:
        # TODO handle exceptions
        raise
    return df_raw

def shape(df: pd.DataFrame):

    create_time = datetime.datetime.now(datetime.timezone.utc)
    col_names = []

    df_shaped = df\
        .set_axis(col_names, axis=1)\
        .assign(create_time=create_time)

    return df

def save_to_db(engine, df: pd.DataFrame):
    df.to_sql('', engine, index = False, if_exists = 'append')

def update(dbapi, dry_run=False):

    url = ''

    engine = create_engine(dbapi)
    df_raw = get(url)
    df = shape(df_raw)

    if dry_run:
        logging.info(df)
    else:
        save_to_db(engine, df)

    return df

if __name__ == '__main__':
    dbapi = sys.argv[1]
    dry_run = sys.argv[2] if len(sys.argv) > 2 else False
    update(dbapi, dry_run)