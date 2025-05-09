import sys
import datetime
import requests
import zipfile
import io
from pathlib import Path
import pandas as pd

from sqlalchemy import create_engine

from common.df_common import basic_shape

from common.utils import (
    graveyard_files,
    list_files,
    dateCETstr_to_tzdt
)

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

# TODO everything below will be deprecated soon by specific tasks for omie

def shape_omie(pathfile, request_time):

    try:
        df = pd.read_csv(pathfile, sep = ';')
    except:
        # TODO handle exceptions
        raise

    noms_columnes = ['year','month','day','hour','price_pt','price']

    df = basic_shape(df, noms_columnes)

    df = df[["date", "price"]]
    df['request_time'] = request_time

    return df

# TODO merge get_historical_hour_price and update_historical_hour_price into one function and one table
def get_historical_hour_price(engine, verbose=2, dry_run=False):

    request_time = datetime.datetime.now(datetime.timezone.utc)

    filetype = 'marginalpdbc'
    hist_files = get_file_list(filetype)
    file_base_url = f'https://www.omie.es/es/file-download?parents={filetype}&filename='

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

                try:
                    df.to_sql('omie_price_hour', engine, index = False, if_exists = 'append')
                except:
                    if verbose > 0:
                        print('error on insert')
                    # TODO handle exceptions
                    raise

    return 0

def update_latest_hour_price(engine, verbose=2, dry_run=False):

    request_time = datetime.datetime.now(datetime.timezone.utc)

    filetype = 'marginalpdbc'
    hist_files = get_file_list(filetype)
    file_base_url = f'https://www.omie.es/es/file-download?parents={filetype}&filename='

    file_name = hist_files['Nombre'][0]
    pathfile = file_base_url + file_name

    if verbose > 1:
        print(f'processing {file_name}')

    if pathfile[-1:] == '1':
        df = shape_omie(pathfile, request_time)
        if dry_run:
            print(df)
        else:

            if verbose > 2:
                print(df)

            try:
                df.to_sql('omie_latest_price_hour', engine, index = False, if_exists = 'replace')
            except:
                if verbose > 0:
                    print('error on insert')
                # TODO handle exceptions
                raise
    return 0

def update_historical_hour_price(engine, verbose=2, dry_run=False):

    request_time = datetime.datetime.now(datetime.timezone.utc)

    filetype = 'marginalpdbc'
    hist_files = get_file_list(filetype)
    file_base_url = f'https://www.omie.es/es/file-download?parents={filetype}&filename='

    file_name = hist_files['Nombre'][0]
    pathfile = file_base_url + file_name

    if verbose > 1:
        print(f'processing {file_name}')

    if pathfile[-1:] == '1':
        df = shape_omie(pathfile, request_time)

        try:
            last_day_on_db = pd.read_sql('select date from omie_price_hour order by date desc limit 1', con=engine)['date'][0]
        except:
            if verbose > 0:
                print('error on fetching latest omie_price_hour')
            # TODO handle exceptions
            raise
        last_day_new_request = max(df['date'])

        if last_day_on_db >= last_day_new_request:
            if verbose > 1:
                print(f'Last record on db is older than requested ({last_day_on_db} >= {last_day_new_request}). Skipping.')
            return 1

        if dry_run:
            print(df)
        else:

            if verbose > 2:
                print(df)

            try:
                df.to_sql('omie_price_hour', engine, index = False, if_exists = 'append')
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

def shape_energy_buy(df, request_time, create_time):

    noms_columnes = ['year','month','day','cardinal_hour','offer_unit_name','demand','res','offer_type','offer_number']
    df = basic_shape(df, noms_columnes)

    df = df[["date", "demand", "offer_number"]] # what to select?
    df['request_time'] = request_time
    df['create_time'] = create_time

    return df

def update_energy_buy_fromweb(engine, verbose=2, dry_run=False):

    filetype = 'pdbc'
    hist_files = get_file_list(filetype)
    file_base_url = f'https://www.omie.es/es/file-download?parents={filetype}&filename='

    file_name = hist_files['Nombre'][0]

    pathfile = file_base_url + file_name

    if verbose > 1:
        print(f'processing newest file of {pathfile}')

    request_time = datetime.datetime.now(datetime.timezone.utc)
    create_time = request_time
    df = download_and_unzip(pathfile)

    if pathfile[-1:] == '1':
        df = shape_energy_buy(df, request_time, create_time)
        if dry_run:
            print(df)
        else:

            if verbose > 2:
                print(df)

            try:
                df.to_sql('omie_energy_buy', engine, index = False, if_exists = 'append')
            except:
                if verbose > 0:
                    print('error on insert')
                # TODO handle exceptions
                raise
    return 0


def update_one_energy_buy(engine, pdbcfile: Path, create_time, verbose=2, dry_run=False):

    filetype, comer, date = pdbcfile.stem.split('_')
    version = pdbcfile.suffix
    request_time = dateCETstr_to_tzdt(date)

    if verbose > 1:
        print(f'processing newest file of {pdbcfile.resolve()}')

    if filetype.upper() != 'PDBC':
        print(f"Wrong file type, expecting PDBC got {filetype}", file=sys.stderr)
        return -2

    try:
        df = pd.read_csv(pdbcfile, sep = ';')
    except:
        # TODO handle exceptions
        raise

    if version == '.1':
        df = shape_energy_buy(df, request_time, create_time)
        if dry_run:
            print(df)
        else:

            if verbose > 2:
                print(df)

            try:
                df.to_sql('omie_energy_buy', engine, index = False, if_exists = 'append')
            except:
                if verbose > 0:
                    print('error on insert')
                # TODO handle exceptions
                raise
    else:
        print(f"Wrong version, expecting 1 got {version}", file=sys.stderr)
        return -3

    return 0

# moves provessed files to graveyard
def update_energy_buy(engine, omie_pdbc_temp_dir, verbose=2, dry_run=False):

    create_time = datetime.datetime.now(datetime.timezone.utc)
    pdbcfiles = list_files(omie_pdbc_temp_dir)

    if not pdbcfiles:
        print(f'No files in {omie_pdbc_temp_dir}.')
        return 1

    results = {f:update_one_energy_buy(engine, f, create_time, verbose, dry_run) for f in pdbcfiles}

    files_ok = [f for f,result in results.items()]

    graveyard_files(omie_pdbc_temp_dir, files_ok, verbose)
    return min(results.values())

# same as update_energy_buy but without moving files, used once in ambits readonly directories
def get_historical_energy_buy(engine, omie_pdbc_dir: Path, verbose=2, dry_run=False):

    create_time = datetime.datetime.now(datetime.timezone.utc)
    pdbcfiles = list_files(omie_pdbc_dir)

    results = [update_one_energy_buy(engine, f, create_time=create_time, verbose=verbose, dry_run=dry_run) for f in pdbcfiles]

    return min(results)