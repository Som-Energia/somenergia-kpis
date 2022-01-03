import pandas as pd
import datetime
import urllib.request, gzip

from sqlalchemy import create_engine

from dbconfig import local_db


# imports
# https://www.meff.es/esp/Derivados-Commodities/Precios-Cierre
# https://www.meff.es/esp/Derivados-Commodities/Precios-Cierre/Excel


# TODO merge get_historical_hour_price and update_historical_hour_price into one function and one table
def get_historical_hour_price(verbose=2, dry_run=False):

    request_time = datetime.datetime.now(datetime.timezone.utc)
    noms_columnes = ['year','month','day','hour','price']
    engine = create_engine(local_db['dbapi'])

    hist_files_url = 'https://www.omie.es/es/file-access-list?parents%5B0%5D=/&parents%5B1%5D=Mercado%20Diario&parents%5B2%5D=1.%20Precios&dir=Precios%20horarios%20del%20mercado%20diario%20en%20Espa%C3%B1a&realdir=marginalpdbc'
    file_base_url = 'https://www.omie.es/es/file-download?parents%5B0%5D=marginalpdbc&filename='

    try:
        hist_files = pd.read_html(hist_files_url)[0]
    except:
        # TODO handle exceptions
        raise

    total = len(hist_files['Nombre'])

    for i,file_name in enumerate(hist_files['Nombre']):

        pathfile = file_base_url + file_name

        if verbose > 1:
            print(f'processing {file_name} {i+1}/{total}')

        if pathfile[-1:] == '1':
            try:
                df_current_day = pd.read_csv(pathfile, sep = ';')
            except:
                # TODO handle exceptions
                raise

            df_current_day_dated = df_current_day\
                .reset_index().iloc[:,:5]\
                .set_axis(noms_columnes, axis=1)\
                .dropna()\
                .assign(date = lambda x: pd.to_datetime(x[['year', 'month', 'day']]))


            first_hour = df_current_day_dated['date'].dt.tz_localize('Europe/Madrid').dt.tz_convert('UTC')[0]
            last_hour = df_current_day_dated['date'].dt.tz_localize('Europe/Madrid').dt.tz_convert('UTC')[0] + pd.Timedelta(hours=len(df_current_day_dated))

            df_current_day_dated['date'] = pd.Series(pd.date_range(first_hour,last_hour,freq='H'))
            df_current_day_dated = df_current_day_dated[["date", "price"]]
            df_current_day_dated['df_current_day_dated'] = request_time

            if dry_run:
                print(df_current_day_dated)
            else:
                try:
                    df_current_day_dated.to_sql('omie_price_hour', engine, index = False, if_exists = 'append')
                except:
                    if verbose > 0:
                        print('error on insert')
                    # TODO handle exceptions
                    raise
    return 0


def update_historical_hour_price(verbose=2, dry_run=False):

    request_time = datetime.datetime.now(datetime.timezone.utc)
    noms_columnes = ['year','month','day','hour','price']
    engine = create_engine(local_db['dbapi'])

    hist_files_url = 'https://www.omie.es/es/file-access-list?parents%5B0%5D=/&parents%5B1%5D=Mercado%20Diario&parents%5B2%5D=1.%20Precios&dir=Precios%20horarios%20del%20mercado%20diario%20en%20Espa%C3%B1a&realdir=marginalpdbc'
    file_base_url = 'https://www.omie.es/es/file-download?parents%5B0%5D=marginalpdbc&filename='

    try:
        hist_files = pd.read_html(hist_files_url)[0]
    except:
        # TODO handle exceptions
        raise

    if verbose > 2:
        print(hist_files['Nombre'][0])

    file_name = hist_files['Nombre'][0]

    pathfile = file_base_url + file_name

    if verbose > 1:
        print(f'processing {file_name}')

    if pathfile[-1:] == '1':
        try:
            df_current_day = pd.read_csv(pathfile, sep = ';')
        except:
            # TODO handle exceptions
            raise

        df_current_day_dated = df_current_day\
            .reset_index().iloc[:,:5]\
            .set_axis(noms_columnes, axis=1)\
            .dropna()\
            .assign(date = lambda x: pd.to_datetime(x[['year', 'month', 'day']]))


        first_hour = df_current_day_dated['date'].dt.tz_localize('Europe/Madrid').dt.tz_convert('UTC')[0]
        last_hour = df_current_day_dated['date'].dt.tz_localize('Europe/Madrid').dt.tz_convert('UTC')[0] + pd.Timedelta(hours=len(df_current_day_dated))

        df_current_day_dated['date'] = pd.Series(pd.date_range(first_hour,last_hour,freq='H'))
        df_current_day_dated = df_current_day_dated[["date", "price"]]
        df_current_day_dated['df_current_day_dated'] = request_time


        if dry_run:
            print(df_current_day_dated)
        else:
            try:
                df_current_day_dated.to_sql('omie_latest_price_hour', engine, index = False, if_exists = 'replace')
            except:
                if verbose > 0:
                    print('error on insert')
                # TODO handle exceptions
                raise
    return 0

