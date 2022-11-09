import sys
import logging

import pandas as pd

import datetime
import urllib.request, gzip
import lxml.html
import requests

from pathlib import Path

from sqlalchemy import create_engine

from .omie_utils import (
    get_file_list,
)

def get_files(engine, filetype, potential_missing):

    pd.DataFrame({
        'date':pd.Series(dtype='datetime64[ns, UTC]'),
        'price':pd.Series(dtype='float'),
        'create_time':pd.Series(dtype='datetime64[ns, UTC]'),
        'file_name':pd.Series(dtype='string'),}
    ).to_sql('omie_historical_price_hour', con=engine, index=False, if_exists='append')

    present = pd.read_sql_query('select distinct(file_name) from omie_historical_price_hour', engine)['file_name']

    potential_missing = [filename for filename in potential_missing if Path(filename).suffix != '.zip']
    missing = set(potential_missing) - set(present)

    if len(missing) < 1:
        print(f'No {filetype} new files available.')
        exit()

    base_url = f'https://www.omie.es/es/file-download?parents%5B0%5D={filetype}&filename='

    try:
        dfs = [
            pd.read_csv(base_url+filename, sep = ';').assign(file_name = filename)
            for filename in missing if Path(filename)
        ]
    except:
        # TODO handle exceptions
        print(f'Unable to get one or more files {missing}')
        raise

    df = pd.concat(dfs)

    return df

def get(engine, filetype):

    potential_missing = get_file_list(filetype)['Nombre']
    return get_files(engine, filetype, potential_missing)

def shape(df: pd.DataFrame):

    create_time = datetime.datetime.now(datetime.timezone.utc)
    col_names = ['year','month','day','hour','price_pt','price','res','file_name']

    df_dated = df\
        .reset_index().iloc[:,:len(col_names)]\
        .set_axis(col_names, axis=1)\
        .assign(create_time=create_time)\
        .drop(columns=['res'])\
        .dropna()\
        .astype({'year': 'int64', 'month': 'int64', 'day': 'int64'})\
        .assign(date = lambda x: pd.to_datetime(x[['year', 'month', 'day']]))\
        .sort_values(by=["year","month",'day','hour'])\
        .reset_index()

    first_hour = df_dated['date'].dt.tz_localize('Europe/Madrid').dt.tz_convert('UTC')[0]
    last_hour = df_dated['date'].dt.tz_localize('Europe/Madrid').dt.tz_convert('UTC')[0] + pd.Timedelta(hours=len(df_dated))

    df_dated['date'] = pd.Series(pd.date_range(first_hour,last_hour,freq='H'))

    df = df_dated[["date", "price", "file_name", "create_time"]]

    return df

def save_to_db(engine, df: pd.DataFrame):
    df.to_sql('omie_historical_price_hour', engine, index = False, if_exists = 'append')

def update(dbapi, dry_run=False):

    filename = 'marginalpdbc'

    engine = create_engine(dbapi)
    df_raw = get(engine, filename)
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