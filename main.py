from pathlib import Path
from typing import List
import enum

import typer
from loguru import logger
from sqlalchemy import create_engine

from datasources.helpscout.hs_get_conversations import update_hs_conversations
from datasources.meff.meff_operations import update_closing_prices_day, update_closing_prices_month
from datasources.neuroenergia.neuroenergia_operations import update_neuroenergia
from datasources.omie.omie_operations import (
    get_historical_energy_buy,
    get_historical_hour_price,
    update_energy_buy,
    update_historical_hour_price,
    update_latest_hour_price,
)
from dbconfig import directories, helpscout_api, local_db
from pipelines.energy_budget import pipe_hourly_energy_budget
from pipelines.omie_garantia import pipe_omie_garantia

app = typer.Typer(context_settings={"help_option_names": ["-h", "--help"]})


def main_update_closing_prices_month(verbose, dry_run):
    engine = None if dry_run else create_engine(local_db["dbapi"])
    return update_closing_prices_month(engine, verbose, dry_run)


def main_update_closing_prices_day(verbose, dry_run):
    engine = None if dry_run else create_engine(local_db["dbapi"])
    return update_closing_prices_day(engine, verbose, dry_run)


def main_update_omie_latest_hour_price(verbose, dry_run):
    engine = None if dry_run else create_engine(local_db["dbapi"])
    return update_latest_hour_price(engine, verbose, dry_run)


def main_get_historical_hour_price(verbose, dry_run):
    engine = None if dry_run else create_engine(local_db["dbapi"])
    return get_historical_hour_price(engine, verbose, dry_run)


def main_update_historical_hour_price(verbose, dry_run):
    engine = create_engine(local_db["dbapi"])
    return update_historical_hour_price(engine, verbose, dry_run)


# TODO Pendent de posar a prod
def main_get_historical_energy_buy(verbose, dry_run):
    raise NotImplementedError
    engine = None if dry_run else create_engine(local_db["dbapi"])
    omie_pdbc_dir = Path(directories["OMIE_HISTORICAL_PDBC"])
    return get_historical_energy_buy(engine, omie_pdbc_dir, verbose, dry_run)


def main_update_energy_buy(verbose, dry_run):
    engine = None if dry_run else create_engine(local_db["dbapi"])
    omie_temp_dir = Path(directories["OMIE_TEMP_PDBC"])
    return update_energy_buy(engine, omie_temp_dir, verbose, dry_run)


def main_update_neuroenergia(verbose, dry_run):
    engine = None if dry_run else create_engine(local_db["dbapi"])
    neuro_dir = directories["NEUROENERGIA_TEMP"]
    return update_neuroenergia(engine, neuro_dir, verbose, dry_run)


def main_update_conversations(verbose, dry_run):
    engine = create_engine(local_db["dbapi"])
    hs_app_id = helpscout_api["app_id"]
    hs_app_secret = helpscout_api["app_secret"]
    return update_hs_conversations(engine, hs_app_id, hs_app_secret, verbose, dry_run)


def main_pipe_hourly_energy_budget(verbose, dry_run):
    engine = None if dry_run else create_engine(local_db["dbapi"])
    return pipe_hourly_energy_budget(engine, verbose)


def main_pipe_omie_garantia(verbose, dry_run):
    engine = None if dry_run else create_engine(local_db["dbapi"])
    return pipe_omie_garantia(engine)


class AvailableFunctions(str, enum.Enum):
    meff_update_closing_prices_day = "meff_update_closing_prices_day"
    meff_update_closing_prices_month = "meff_update_closing_prices_month"
    omie_get_historical_hour_price = "omie_get_historical_hour_price"
    omie_update_latest_hour_price = "omie_update_latest_hour_price"
    omie_update_historical_hour_price = "omie_update_historical_hour_price"
    omie_get_historical_energy_buy = "omie_get_historical_energy_buy"
    omie_update_energy_buy = "omie_update_energy_buy"
    neuro_update_energy_prediction = "neuro_update_energy_prediction"
    hs_update_conversations = "hs_update_conversations"
    pipe_hourly_energy_budget = "pipe_hourly_energy_budget"
    pipe_omie_garantia = "pipe_omie_garantia"

    @classmethod
    def pretty_list(cls):
        return [f.value for f in cls]

    @classmethod
    def get_function(cls, function_name):
        __dispatcher = {
            "meff_update_closing_prices_day": main_update_closing_prices_day,
            "meff_update_closing_prices_month": main_update_closing_prices_month,
            "omie_get_historical_hour_price": main_get_historical_hour_price,
            "omie_update_latest_hour_price": main_update_omie_latest_hour_price,
            "omie_update_historical_hour_price": main_update_historical_hour_price,
            "omie_get_historical_energy_buy": main_get_historical_energy_buy,
            "omie_update_energy_buy": main_update_energy_buy,
            "neuro_update_energy_prediction": main_update_neuroenergia,
            "hs_update_conversations": main_update_conversations,
            "pipe_hourly_energy_budget": main_pipe_hourly_energy_budget,
            "pipe_omie_garantia": main_pipe_omie_garantia,
        }

        return __dispatcher[function_name]


@app.command()
def dispatch(
    function: List[AvailableFunctions] = typer.Argument(..., help="Choose which function you want to run."),
    verbose: int = typer.Option(2, "-v", "--verbose", count=True, help="Increase verbosity"),
    list_functions: bool = typer.Option(False, "-l", "--list-functions", is_flag=True, help="List available functions"),
    dry_run: bool = typer.Option(False, "-s", "--dry-run", is_flag=True, help="Show dataframes, but don't save to db"),
):
    if verbose > 1:
        logger.info("Start operations")

    if list_functions:
        logger.info(f"Available functions: {AvailableFunctions.pretty_list()}")
        return 0

    results = []

    for one_function in function:
        logger.info(f"Running {one_function}")
        operation_function = AvailableFunctions.get_function(one_function)

        if operation_function:
            result = operation_function(verbose, dry_run)
            if result != 0 and verbose > 0 or result and verbose > 1:
                logger.info(f"{one_function} ended with result {result}")
        else:
            if verbose > 0:
                logger.info(f"{function} not found. options: {AvailableFunctions.pretty_list()}")
            result = -1

        results.append(result)

    if verbose > 1:
        logger.info(results or "No results to show")
        logger.info("Job's Done, Have a Nice Day")
    return results


if __name__ == "__main__":
    app()
