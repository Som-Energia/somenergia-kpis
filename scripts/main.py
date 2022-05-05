import click
import dbconfig
from clean_postal_code import (
    normalize_street_address,
)
from erppeek import Client
from sqlalchemy import create_engine

def normalize_street_address():
    client = Client(**dbconfig.erppeek_testing)
    engine = create_engine(dbconfig.test_db['dbapi'])
    normalize_street_address(engine, client)

function_list = {
    # Descarga de datos erp
    "Normalize street address": normalize_street_address,
    # Carga de datos en dataframe
    # Normalizar datos de direcciones
}

@click.command()
@click.option('-f','--function', type=str, default=None, help='Choose which function you want to run.')
@click.option('-l','--list-functions', default=False, is_flag=True)

def dispatch(function, list_functions):
    if list_functions:
        print(f'Available functions {list(function_list.keys())}')
        return 0

    results = []

    for one_function in function:
        operation_function = function_list.get(one_function, None)

        if operation_function:
            result = operation_function
            if result != 0:
                print(f'{function} ended with result {result}')
        else:
            print(f'{function} not found. options: {list(function_list.keys())}')
            result = -1
        results.append(result)
    return results

if __name__ == '__main__':
    results = dispatch()