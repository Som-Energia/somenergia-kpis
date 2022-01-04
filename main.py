import sys
import click
from meff.meff_operations import (
    update_closing_prices_day,
    update_closing_prices_month
)
from omie.omie_operations import (
    get_historical_hour_price,
    update_latest_hour_price
)

function_list = {
    'update_closing_prices_day': update_closing_prices_day,
    'update_closing_prices_month': update_closing_prices_month,
    'get_historical_hour_price': get_historical_hour_price,
    'update_latest_hour_price': update_latest_hour_price
}

# TODO use logging instead of -v
@click.command()
@click.option('-f','--function', type=str, default=None, help='Choose which function you want to run.', multiple=True)
@click.option('-v','--verbose', default=2, count=True)
@click.option('-l','--list-functions', default=False, is_flag=True)
@click.option('-s','--dry-run', default=False, is_flag=True, help='Show dataframes but dont save to db')
def dispatch(function, verbose, list_functions, dry_run):

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
        print("Job's Done, Have a Nice Day")
    return results

if __name__ == '__main__':
    results = dispatch()
    final_result = min(results)
    sys.exit(final_result)
