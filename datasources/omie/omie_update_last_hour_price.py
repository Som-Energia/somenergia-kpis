import datetime
import ssl
import sys
from pathlib import Path

import pandas as pd
from loguru import logger
from sqlalchemy import create_engine

from .omie_utils import (
    get_file_list,
)


class WrongFormatException(Exception):
    pass


def get_files(engine, filetype, potential_missing):

    pd.DataFrame(
        {
            "date": pd.Series(dtype="datetime64[ns, UTC]"),
            "price": pd.Series(dtype="float"),
            "create_time": pd.Series(dtype="datetime64[ns, UTC]"),
            "file_name": pd.Series(dtype="string"),
        }
    ).to_sql("omie_historical_price_hour", con=engine, index=False, if_exists="append")

    present = pd.read_sql_query(
        "select distinct(file_name) from omie_historical_price_hour", engine
    )["file_name"]

    potential_missing = [
        filename for filename in potential_missing if Path(filename).suffix != ".zip"
    ]
    missing = set(potential_missing) - set(present)

    if len(missing) < 1:
        print(f"No {filetype} new files available.")
        exit()

    base_url = f"https://www.omie.es/es/file-download?parents={filetype}&filename="

    try:
        dfs = [
            pd.read_csv(base_url + filename, sep=";").assign(file_name=filename)
            for filename in missing
            if Path(filename)
        ]
    except:
        # TODO handle exceptions
        print(f"Unable to get one or more files {missing}")
        raise

    df = pd.concat(dfs)

    return df


def get(engine, filetype):

    potential_missing = get_file_list(filetype)["Nombre"]
    return get_files(engine, filetype, potential_missing)


def shape(df: pd.DataFrame):

    create_time = datetime.datetime.now(datetime.timezone.utc)
    col_names = [
        "year",
        "month",
        "day",
        "period",
        "price_pt",
        "price",
        "res",
        "file_name",
    ]

    df_dated = (
        df.reset_index()
        .iloc[:, : len(col_names)]
        .set_axis(col_names, axis=1)
        .assign(create_time=create_time)
        .drop(columns=["res"])
        .dropna()
        .astype({"year": "int64", "month": "int64", "day": "int64"})
        .assign(date=lambda x: pd.to_datetime(x[["year", "month", "day"]]))
        .sort_values(by=["year", "month", "day", "period"])
        .reset_index()
    )

    MIN_QH_DAY = 92  # noqa: F841
    MAX_QH_DAY = 100  # noqa: F841

    MIN_H_DAY = 23  # noqa: F841
    MAX_H_DAY = 25  # noqa: F841

    df_dated_by_day = (
        df_dated.groupby(["year", "month", "day"]).size().reset_index(name="counts")
    )

    if not df_dated_by_day.query("@MIN_H_DAY <= counts and counts <= @MAX_H_DAY").empty:
        period_time_delta = pd.Timedelta(hours=len(df_dated))
        freq = "H"
    elif not df_dated_by_day.query(
        "@MIN_QH_DAY <= counts and counts <= @MAX_QH_DAY"
    ).empty:
        period_time_delta = pd.Timedelta(minutes=15 * len(df_dated))
        freq = "15min"
    else:
        # We expect either an hourly or a quarter-hourly shaped dataframe with daylight saving time support
        raise WrongFormatException

    period_start = (
        df_dated["date"].dt.tz_localize("Europe/Madrid").dt.tz_convert("UTC")[0]
    )
    period_end = (
        df_dated["date"].dt.tz_localize("Europe/Madrid").dt.tz_convert("UTC")[0]
        + period_time_delta
    )

    df_dated["date"] = pd.Series(pd.date_range(period_start, period_end, freq=freq))

    df = df_dated[["date", "price", "file_name", "create_time"]]

    return df


def save_to_db(engine, df: pd.DataFrame):
    df.to_sql("omie_historical_price_hour", engine, index=False, if_exists="append")


def update(dbapi, dry_run=False):

    filename = "marginalpdbc"

    engine = create_engine(dbapi)
    df_raw = get(engine, filename)
    df = shape(df_raw)

    if dry_run:
        logger.info(df)
    else:
        save_to_db(engine, df)
        logger.info(f"Inserted {len(df)} rows")

    return df


if __name__ == "__main__":
    ssl._create_default_https_context = ssl._create_stdlib_context
    dbapi = sys.argv[1]
    dry_run = sys.argv[2] if len(sys.argv) > 2 else False
    update(dbapi, dry_run)
