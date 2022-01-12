import pandas as pd
import datetime
import urllib.request, gzip
import pytz

from sqlalchemy import create_engine

from common.utils import (
    graveyard_files,
    list_files,
    dateCETstr_to_tzdt
)

# imports
# https://www.meff.es/esp/Derivados-Commodities/Precios-Cierre
# https://www.meff.es/esp/Derivados-Commodities/Precios-Cierre/Excel

def shape_neuroenergia(neuro_df, request_time_dt, create_time_dt, verbose=2):
    noms_columnes = ['date','hour','base','market']

    df_energy_forecast = neuro_df\
        .iloc[2:,:len(noms_columnes)]\
        .set_axis(noms_columnes, axis=1)\
        .assign(
            request_time = lambda x: request_time_dt,
            create_time = lambda x: create_time_dt,
            date = lambda x: pd.to_datetime(x['date'], format='%Y-%m-%d').dt.date
        )\
        .query(f"{noms_columnes[2]} != 0 & {noms_columnes[3]} != 0")\
        .astype({'base': float, 'market': float})\
        .reset_index(drop=True)\
        .filter(['date','base','market','request_time','create_time'])

    time_series = pd.date_range(
            start=df_energy_forecast['date'].iloc[0],
            end=df_energy_forecast['date'].iloc[-1]+datetime.timedelta(days=1),
            tz='Europe/Madrid',
            closed='left',
            freq='H'
    )

    try:
        df_energy_forecast['date'] = time_series
    except ValueError:
        print("Missing expected values for date hours. Review your neurofile.")
        raise

    return df_energy_forecast

def neurofile_to_date(neurofile):

    request_date_str = neurofile.stem.split('_')[0]
    request_time = dateCETstr_to_tzdt(request_date_str)
    return request_time

def update_one_neuroenergia(engine, neurofile, create_time, verbose=2, dry_run=False):

    request_time = neurofile_to_date(neurofile)

    try:
        neuro_df = pd.read_excel(neurofile)
    except:
        # TODO handle exceptions
        raise

    if verbose > 2:
        print(neuro_df)

    df_energy_forecast = shape_neuroenergia(neuro_df, request_time, create_time)

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

        graveyard_files(directory=neurofile.parent, files=[neurofile], verbose=verbose)

    return 0

def update_neuroenergia(engine, neuro_dir, verbose=2, dry_run=False):

    create_time = datetime.datetime.now(datetime.timezone.utc)

    neurofiles = list_files(neuro_dir)

    if not neurofiles:
        print(f'No new files to process in {neuro_dir}')
        return 1

    results = {neurofile: update_one_neuroenergia(engine, neurofile, create_time, verbose, dry_run) for neurofile in neurofiles}

    worst_result = min(results.values())

    return worst_result
