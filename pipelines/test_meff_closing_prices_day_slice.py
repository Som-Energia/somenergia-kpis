import unittest

import pandas as pd
from pandas.testing import assert_frame_equal

from pipelines import (
    meff_closing_prices_day_slice,
    meff_closing_prices_month_slice,
)

class MeffOperationsSliceTest(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        # TODO destroy db
        pass

    def test__slice_closing_prices_day__base(self):

        raw_prices = pd.read_csv('testdata/inputdata/closing_prices_lake.csv', sep=';', parse_dates=['create_time'], date_parser=lambda col: pd.to_datetime(col, utc=True))

        meff_df = meff_closing_prices_day_slice.transform(raw_prices)

        # Note: read_csv with date_parser converts each row as element which returns a Timestamp
        # in the transform function we parse the whole Series (column) which returns object dtype
        # Therefore we need to read and then parse otherwise dtypes would differ.
        goal_prices = pd.read_csv('testdata/inputdata/closing_prices_transformed_day.csv', sep = ';')\
            .assign(
                price_date = lambda x: pd.to_datetime(x.price_date).dt.date,
                emission_date = lambda x: pd.to_datetime(x.emission_date).dt.date,
                )

        assert_frame_equal(meff_df.drop(columns='create_time', axis=1), goal_prices.drop(columns='create_time', axis=1))


    def test__slice_closing_prices_day__duplicated_emissions(self):

        raw_prices = pd.read_csv('testdata/inputdata/closing_prices_lake_duplicated_emissions.csv', sep=';', parse_dates=['create_time'], date_parser=lambda col: pd.to_datetime(col, utc=True))

        meff_df = meff_closing_prices_day_slice.transform(raw_prices)

        # Note: read_csv with date_parser converts each row as element which returns a Timestamp
        # in the transform function we parse the whole Series (column) which returns object dtype
        # Therefore we need to read and then parse otherwise dtypes would differ.
        goal_prices = pd.read_csv('testdata/inputdata/closing_prices_transformed_day_duplicated_emissions.csv', sep = ';')\
            .assign(
                price_date = lambda x: pd.to_datetime(x.price_date).dt.date,
                emission_date = lambda x: pd.to_datetime(x.emission_date).dt.date,
                )

        assert_frame_equal(meff_df.drop(columns='create_time', axis=1), goal_prices.drop(columns='create_time', axis=1))

    def test__slice_closing_prices_month__base(self):

        raw_prices = pd.read_csv('testdata/inputdata/closing_prices_lake.csv', sep=';', parse_dates=['create_time'], date_parser=lambda col: pd.to_datetime(col, utc=True))

        meff_df = meff_closing_prices_month_slice.transform(raw_prices)

        # Note: read_csv with date_parser converts each row as element which returns a Timestamp
        # in the transform function we parse the whole Series (column) which returns object dtype
        # Therefore we need to read and then parse otherwise dtypes would differ.
        goal_prices = pd.read_csv('testdata/inputdata/closing_prices_transformed_month.csv', sep = ';')\
            .assign(
                price_date = lambda x: pd.to_datetime(x.price_date).dt.date,
                emission_date = lambda x: pd.to_datetime(x.emission_date).dt.date,
                )

        assert_frame_equal(meff_df.drop(columns='create_time', axis=1), goal_prices.drop(columns='create_time', axis=1))

    def test__slice_closing_prices_month__duplicated_emissions(self):

        raw_prices = pd.read_csv('testdata/inputdata/closing_prices_lake_duplicated_emissions.csv', sep=';', parse_dates=['create_time'], date_parser=lambda col: pd.to_datetime(col, utc=True))

        meff_df = meff_closing_prices_month_slice.transform(raw_prices)

        # Note: read_csv with date_parser converts each row as element which returns a Timestamp
        # in the transform function we parse the whole Series (column) which returns object dtype
        # Therefore we need to read and then parse otherwise dtypes would differ.
        goal_prices = pd.read_csv('testdata/inputdata/closing_prices_transformed_month_duplicated_emissions.csv', sep = ';')\
            .assign(
                price_date = lambda x: pd.to_datetime(x.price_date).dt.date,
                emission_date = lambda x: pd.to_datetime(x.emission_date).dt.date,
                )

        assert_frame_equal(meff_df.drop(columns='create_time', axis=1), goal_prices.drop(columns='create_time', axis=1))

    def test__slice_closing_prices_month__emissions_as_null(self):

        raw_prices = pd.read_csv('testdata/inputdata/closing_prices_lake_duplicated_emissions.csv', sep=';', parse_dates=['create_time'], date_parser=lambda col: pd.to_datetime(col, utc=True))

        raw_prices['emission_date'][228:300] = None

        meff_df = meff_closing_prices_month_slice.transform(raw_prices)

        goal_prices = pd.read_csv('testdata/inputdata/closing_prices_transformed_month_null_emissions.csv', sep = ';')\
            .assign(
                price_date = lambda x: pd.to_datetime(x.price_date).dt.date,
                emission_date = lambda x: pd.to_datetime(x.emission_date).dt.date,
                )

        assert_frame_equal(meff_df.drop(columns='create_time', axis=1), goal_prices.drop(columns='create_time', axis=1))