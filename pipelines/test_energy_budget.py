import unittest

import pandas as pd
from sqlalchemy import MetaData, Table, create_engine

import dbconfig
from common.utils import dateCETstr_to_CETtzdt, dateCETstr_to_tzdt
from pipelines.energy_budget import (
    hourly_energy_budget,
    interpolated_last_meff_prices_by_hour,
    joined_timeseries,
)


class NeuroenergiaOperationsTest(unittest.TestCase):

    from b2btest.b2btest import assertB2BEqual

    def setUp(self):
        self.b2bdatapath = "testdata/b2bdata"
        self.engine = create_engine(dbconfig.test_db["dbapi"])
        self.drop_test_database()

    def tearDown(self):
        self.drop_test_database()

    def drop_test_database(self):
        # TODO Drop all existing tables in test db at once
        Table("meff_precios_cierre_dia", MetaData()).drop(self.engine, checkfirst=True)

    def create_datasources(self):
        # TODO create test omie buy, prices table, meff and neuro
        pass

    def test__interpolated_last_meff_prices_by_hour(self):

        meff_df = pd.read_csv(
            "testdata/inputdata/meff_precios_cierre_dia_to_interpolate.csv",
            parse_dates=["date", "request_time"],
        )

        meff_df = interpolated_last_meff_prices_by_hour(meff_df)

        self.assertB2BEqual(meff_df.to_csv(index=False))

    def test__joined_timeseries(self):

        df_1 = pd.DataFrame(
            {
                "date": [
                    dateCETstr_to_tzdt("2022-01-04 00:00:00", "%Y-%m-%d %H:%M:%S"),
                    dateCETstr_to_tzdt("2022-01-04 01:00:00", "%Y-%m-%d %H:%M:%S"),
                ],
                "price": [289, 345],
            }
        )

        df_2 = pd.DataFrame(
            {
                "date": [
                    dateCETstr_to_tzdt("2022-01-04 00:00:00", "%Y-%m-%d %H:%M:%S"),
                    dateCETstr_to_tzdt("2022-01-04 01:00:00", "%Y-%m-%d %H:%M:%S"),
                ],
                "energy": [1398, 1675],
            }
        )

        dfs = [df_1, df_2]

        result = joined_timeseries(dfs)

        self.assertB2BEqual(result.to_csv(index=False, na_rep="NaN"))

    def _test__joined_timeseries__repeated_column_name(self):
        pass

    def test__hourly_energy_budget(self):

        df = pd.DataFrame(
            {
                "date": [
                    dateCETstr_to_CETtzdt("2022-01-04 00:00:00", "%Y-%m-%d %H:%M:%S"),
                    dateCETstr_to_CETtzdt("2022-01-04 01:00:00", "%Y-%m-%d %H:%M:%S"),
                ],
                "price": [250, 350],
                "energy": [1000, 1100],
                "price_forecast": [300, 400],
                "energy_forecast": [700, 800],
            }
        )

        df = hourly_energy_budget(df)

        self.assertB2BEqual(df.to_csv(index=False))
