import unittest

import pandas as pd
from pandas.testing import assert_frame_equal

from pipelines.meff_closing_prices_day_slice import (
    transform,
)

class MeffOperationsSliceTest(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        # TODO destroy db
        pass

    def test__slice_closing_prices(self):

        raw_prices = pd.read_csv('testdata/inputdata/closing_praces_lake.csv', sep=';', parse_dates=['create_time'], date_parser=lambda col: pd.to_datetime(col, utc=True))

        meff_df = transform(raw_prices)

        # Note: read_csv with date_parser converts each row as element which returns a Timestamp
        # in the transform function we parse the whole Series (column) which returns object dtype
        # Therefore we need to read and then parse otherwise dtypes would differ.
        goal_prices = pd.read_csv('testdata/inputdata/closing_prices_transformed.csv', sep = ';')\
            .assign(
                price_date = lambda x: pd.to_datetime(x.price_date).dt.date,
                emission_date = lambda x: pd.to_datetime(x.emission_date).dt.date,
                )

        assert_frame_equal(meff_df.drop(columns='create_time', axis=1), goal_prices.drop(columns='create_time', axis=1))