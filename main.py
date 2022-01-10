import sys
import click
from pathlib import Path

from sqlalchemy import create_engine
from dbconfig import (
    local_db,
    directories
)

from meff.meff_operations import (
    update_closing_prices_day,
    update_closing_prices_month
)
from omie.omie_operations import (
    get_historical_hour_price,
    update_latest_hour_price,
    update_energy_buy,
    get_historical_energy_buy,
    update_historical_hour_price
)
from neuroenergia.neuroenergia_operations import (
    update_neuroenergia
)


import datetime

def main_update_closing_prices_month(verbose, dry_run):
    engine = None if dry_run else create_engine(local_db['dbapi'])
    update_closing_prices_month(engine, verbose, dry_run)

def main_update_closing_prices_day(verbose, dry_run):
    engine = None if dry_run else create_engine(local_db['dbapi'])
    update_closing_prices_day(engine, verbose, dry_run)

def main_update_omie_latest_hour_price(verbose, dry_run):
    engine = None if dry_run else create_engine(local_db['dbapi'])
    update_latest_hour_price(engine, verbose, dry_run)

def main_get_historical_hour_price(verbose, dry_run):
    engine = None if dry_run else create_engine(local_db['dbapi'])
    get_historical_hour_price(engine, verbose, dry_run)

def main_update_historical_hour_price(verbose, dry_run):
    engine = create_engine(local_db['dbapi'])
    update_historical_hour_price(engine, verbose, dry_run)

# TODO Pendent de posar a prod
def main_get_historical_energy_buy(verbose, dry_run):
    engine = None if dry_run else create_engine(local_db['dbapi'])
    omie_pdbc_dir = Path(directories['OMIE_HISTORICAL_PDBC'])
    get_historical_energy_buy(engine, omie_pdbc_dir, verbose, dry_run)

def main_update_energy_buy(verbose, dry_run):
    engine = None if dry_run else create_engine(local_db['dbapi'])
    omie_temp_dir = Path(directories['OMIE_TEMP_PDBC'])
    update_energy_buy(engine, omie_temp_dir, verbose, dry_run)

def main_update_neuroenergia(verbose, dry_run):
    engine = None if dry_run else create_engine(local_db['dbapi'])
    neuro_dir = directories['NEUROENERGIA']
    update_neuroenergia(engine,neuro_dir,verbose,dry_run)

function_list = {
    'update_closing_prices_day': main_update_closing_prices_day,
    'update_closing_prices_month': main_update_closing_prices_month,
    'get_historical_hour_price': main_get_historical_hour_price,
    'update_latest_hour_price': main_update_omie_latest_hour_price,
    'update_historical_hour_price': main_update_historical_hour_price,
    'get_historical_energy_buy' : main_get_historical_energy_buy,
    'update_energy_buy' : main_update_energy_buy,
    'update_neuroenergia' : main_update_neuroenergia
}

# TODO use logging instead of -v
@click.command()
@click.option('-f','--function', type=str, default=None, help='Choose which function you want to run.', multiple=True)
@click.option('-v','--verbose', default=2, count=True)
@click.option('-l','--list-functions', default=False, is_flag=True)
@click.option('-s','--dry-run', default=False, is_flag=True, help='Show dataframes but dont save to db')
def dispatch(function, verbose, list_functions, dry_run):

    if verbose > 1:
        print(f"[{datetime.datetime.now()}] Start operations")

    if list_functions:
        print(f'Available functions {list(function_list.keys())}')
        return 0

    results = []

    for one_function in function:
        operation_function = function_list.get(one_function, None)

        if operation_function:
            result = operation_function(verbose, dry_run)
            if result != 0 and verbose > 0 or result and verbose > 1:
                print(f'{function} ended with result {result}')
        else:
            if verbose > 0:
                print(f'{function} not found. options: {list(function_list.keys())}')
            result = -1

        results.append(result)

    if verbose > 1:
        print(results)
        print(f"[{datetime.datetime.now()}] Job's Done, Have a Nice Day")
    return results

if __name__ == '__main__':
    results = dispatch()
    final_result = min(results)
    sys.exit(final_result)
