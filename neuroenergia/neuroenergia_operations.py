import pandas as pd
import datetime
import urllib.request, gzip
import pytz

from sqlalchemy import create_engine

from common.utils import (
    list_files,
    dateCETstr_to_tzdt
)

from dbconfig import (
    local_db,
    directories
)

# imports
# https://www.meff.es/esp/Derivados-Commodities/Precios-Cierre
# https://www.meff.es/esp/Derivados-Commodities/Precios-Cierre/Excel

def shape_neuroenergia(neuro_df, noms_columnes, request_time, create_time, verbose=2):

    df_energy_forecast = neuro_df\
        .iloc[2:,:len(noms_columnes)]\
        .set_axis(noms_columnes, axis=1)\
        .set_index('date', drop=False)\
        .assign(
            request_time = lambda x: request_time,
            create_time = lambda x: create_time,
            date = lambda x: pd.to_datetime(x['date'], format='%Y-%m-%d').dt.date,
        )

    return df_energy_forecast

def neurofile_to_date(neurofile):

    request_date_str = neurofile.stem.split('_')[0]
    request_time = dateCETstr_to_tzdt(request_date_str)
    return request_time

def update_one_neuroenergia(engine, neurofile, create_time, verbose=2, dry_run=False):

    noms_columnes = ['date','hour','base','market']
    request_time = neurofile_to_date(neurofile)

    try:
        neuro_df = pd.read_excel(neurofile)
    except:
        # TODO handle exceptions
        raise

    if verbose > 2:
        print(neuro_df)

    df_energy_forecast = shape_neuroenergia(neuro_df, noms_columnes, request_time, create_time)

    if dry_run:
        print(df_energy_forecast)
    else:
        if verbose > 2:
            print(df_energy_forecast)
        try:
            df_energy_forecast.to_sql('energy_buy_forecast', engine, index = False, if_exists = 'append')
        except:
            # TODO handle exceptions
            raise

    return 0

def update_neuroenergia(verbose=2, dry_run=False):

    neuro_dir = directories['NEUROENERGIA']
    engine = create_engine(local_db['dbapi'])
    create_time = datetime.datetime.now(datetime.timezone.utc)

    neurofiles = list_files(neuro_dir)

    if not neurofiles:
        print(f'No new files to process in {neuro_dir}')

    results = [update_one_neuroenergia(engine, neurofile, create_time, verbose, dry_run) for neurofile in neurofiles]

    worst_result = min(results)

    return worst_result
