from os import path
import pandas as pd
import datetime
import requests
import zipfile
import io

from sqlalchemy import create_engine

from dbconfig import local_db

def get_file_list(filetype, verbose=2):

    file_listing_base_url = 'https://www.omie.es/es/file-access-list'
    hist_files_url = '{}?realdir={}'.format(file_listing_base_url, filetype)

    try:
        hist_files = pd.read_html(hist_files_url)[0]
    except:
        # TODO handle exceptions
        raise

    if verbose > 2:
        print(hist_files['Nombre'][0])

    return hist_files

def base_shape(df, noms_columnes):
    df_dated = df\
        .reset_index().iloc[:,:len(noms_columnes)]\
        .set_axis(noms_columnes, axis=1)\
        .dropna()\
        .assign(date = lambda x: pd.to_datetime(x[['year', 'month', 'day']]))

    first_hour = df_dated['date'].dt.tz_localize('Europe/Madrid').dt.tz_convert('UTC')[0]
    last_hour = df_dated['date'].dt.tz_localize('Europe/Madrid').dt.tz_convert('UTC')[0] + pd.Timedelta(hours=len(df_dated))

    df_dated['date'] = pd.Series(pd.date_range(first_hour,last_hour,freq='H'))

    return df_dated

def shape_omie(pathfile, request_time):

    try:
        df = pd.read_csv(pathfile, sep = ';')
    except:
        # TODO handle exceptions
        raise

    noms_columnes = ['year','month','day','hour','price_pt','price']
    df = base_shape(df, noms_columnes)
    df = df[["date", "price"]]
    df['df_current_day_dated'] = request_time

    return df

# TODO merge get_historical_hour_price and update_historical_hour_price into one function and one table
def get_historical_hour_price(verbose=2, dry_run=False):

    request_time = datetime.datetime.now(datetime.timezone.utc)

    filetype = 'marginalpdbc'
    hist_files = get_file_list(filetype)
    file_base_url = f'https://www.omie.es/es/file-download?parents%5B0%5D={filetype}&filename='

    total = len(hist_files['Nombre'])

    for i,file_name in enumerate(hist_files['Nombre']):

        pathfile = file_base_url + file_name

        if verbose > 1:
            print(f'processing {file_name} {i+1}/{total}')

        if pathfile[-1:] == '1':
            df = shape_omie(pathfile, request_time)

            if dry_run:
                print(df)
            else:
                engine = create_engine(local_db['dbapi'])

                try:
                    df.to_sql('omie_price_hour', engine, index = False, if_exists = 'append')
                except:
                    if verbose > 0:
                        print('error on insert')
                    # TODO handle exceptions
                    raise

    return 0

def update_latest_hour_price(verbose=2, dry_run=False):

    request_time = datetime.datetime.now(datetime.timezone.utc)

    filetype = 'marginalpdbc'
    hist_files = get_file_list(filetype)
    file_base_url = f'https://www.omie.es/es/file-download?parents%5B0%5D={filetype}&filename='

    file_name = hist_files['Nombre'][0]
    pathfile = file_base_url + file_name

    if verbose > 1:
        print(f'processing {file_name}')

    if pathfile[-1:] == '1':
        df = shape_omie(pathfile, request_time)
        if dry_run:
            print(df)
        else:
            engine = create_engine(local_db['dbapi'])

            try:
                df.to_sql('omie_latest_price_hour', engine, index = False, if_exists = 'replace')
            except:
                if verbose > 0:
                    print('error on insert')
                # TODO handle exceptions
                raise
    return 0

def download_and_unzip(pathfile, file_index=None):
    file_index = file_index or -1

    r = requests.get(pathfile)
    with zipfile.ZipFile(io.BytesIO(r.content)) as z:
        filenames = z.namelist()
        filename = filenames[-1]
        # if we only need latest
        with z.open(filename) as zfile:
           df = pd.read_csv(zfile, sep = ';')

    return df

def shape_energy_buy(df, request_time):

    noms_columnes = ['year','month','day','hour','text_id','var','flag1','flag2','ref']
    df = base_shape(df, noms_columnes)
    df = df[["date", "text_id", "var"]] # what to select?
    df['request_time'] = request_time

    return df

def update_energy_buy_fromweb(verbose=2, dry_run=False):

    filetype = 'pdbc'
    hist_files = get_file_list(filetype)
    file_base_url = f'https://www.omie.es/es/file-download?parents%5B0%5D={filetype}&filename='

    file_name = hist_files['Nombre'][0]

    pathfile = file_base_url + file_name

    if verbose > 1:
        print(f'processing newest file of {pathfile}')

    request_time = datetime.datetime.now(datetime.timezone.utc)
    df = download_and_unzip(pathfile)

    if pathfile[-1:] == '1':
        df = shape_energy_buy(df, request_time)
        if dry_run:
            print(df)
        else:
            engine = create_engine(local_db['dbapi'])

            try:
                df.to_sql('omie_energy_buy', engine, index = False, if_exists = 'append')
            except:
                if verbose > 0:
                    print('error on insert')
                # TODO handle exceptions
                raise
    return 0


def update_energy_buy(verbose=2, dry_run=False):

    filetype = 'pdbc'
    hist_files = get_file_list(filetype)
    file_base_url = f'https://www.omie.es/es/file-download?parents%5B0%5D={filetype}&filename='

    file_name = hist_files['Nombre'][0]

    pathfile = file_base_url + file_name

    if verbose > 1:
        print(f'processing newest file of {pathfile}')

    request_time = datetime.datetime.now(datetime.timezone.utc)
    df = download_and_unzip(pathfile)

    if pathfile[-1:] == '1':
        df = shape_energy_buy(df, request_time)
        if dry_run:
            print(df)
        else:
            engine = create_engine(local_db['dbapi'])

            try:
                df.to_sql('omie_energy_buy', engine, index = False, if_exists = 'append')
            except:
                if verbose > 0:
                    print('error on insert')
                # TODO handle exceptions
                raise
    return 0