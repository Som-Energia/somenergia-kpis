import unittest
from unittest.case import skipIf

import pandas as pd
from pandas.testing import assert_frame_equal
from sqlalchemy import create_engine

from datasources.omie.omie_operations import get_file_list
from datasources.omie.omie_update_last_hour_price import (
    WrongFormatException,
    get_files,
    shape,
)


class OmieUpdateTest(unittest.TestCase):

    @skipIf(True, "downloads from website, maybe don't abuse it")
    def test_get_file_list(self):

        hist_files = get_file_list("marginalpdbc")

        self.assertIn("Nombre", hist_files.columns)
        self.assertTrue(len(hist_files) > 0)
        self.assertEqual(hist_files["Nombre"][0][-1:], "1")

    def test__shape_omie__hourly(self):

        filepath = "testdata/MARGINALPDBC/marginalpdbc_20211213.1_to_shape"

        omie_df = pd.read_csv(filepath, sep=";").set_index(
            ["level_0", "level_1", "level_2", "level_3", "level_4"]
        )

        df = shape(omie_df)

        expected = pd.read_csv(
            "testdata/inputdata/omie.test_omie_operations.OmieOperationsTest.test__shape_omie-expected.csv",
            sep=";",
            parse_dates=["date", "create_time"],
            date_parser=lambda col: pd.to_datetime(col, utc=True),
        )

        assert_frame_equal(
            df.drop(columns="create_time", axis=1),
            expected.drop(columns="create_time", axis=1),
        )

    def test__shape_omie__unexpected_rows(self):

        filepath = (
            "testdata/MARGINALPDBC/marginalpdbc_20211213.1_to_shape__unexpected_rows"
        )

        omie_df = pd.read_csv(filepath, sep=";").set_index(
            ["level_0", "level_1", "level_2", "level_3", "level_4"]
        )

        with self.assertRaises(WrongFormatException):
            shape(omie_df)

    def test__shape_omie__dst(self):

        filepath = "testdata/MARGINALPDBC/marginalpdbc_20251026.1_dst"

        omie_df = pd.read_csv(filepath, sep=";").set_index(
            ["level_0", "level_1", "level_2", "level_3", "level_4"]
        )

        df = shape(omie_df)

        expected = pd.read_csv(
            "testdata/inputdata/omie.test_omie_operations.OmieOperationsTest.test__shape_omie__dst-expected.csv",
            sep=";",
            parse_dates=["date", "create_time"],
            date_parser=lambda col: pd.to_datetime(col, utc=True),
        )

        assert_frame_equal(
            df.drop(columns="create_time", axis=1),
            expected.drop(columns="create_time", axis=1),
        )

    def test__shape_omie__qh(self):

        filepath = "testdata/MARGINALPDBC/marginalpdbc_20251031.1_qh"

        omie_df = pd.read_csv(filepath, sep=";").set_index(
            ["level_0", "level_1", "level_2", "level_3", "level_4"]
        )

        df = shape(omie_df)

        expected = pd.read_csv(
            "testdata/inputdata/omie.test_omie_operations.OmieOperationsTest.test__shape_omie__qh-expected.csv",
            sep=";",
            parse_dates=["date", "create_time"],
            date_parser=lambda col: pd.to_datetime(col, utc=True),
        )

        assert_frame_equal(
            df.drop(columns="create_time", axis=1),
            expected.drop(columns="create_time", axis=1),
        )


class OmieUpdateDBTest(unittest.TestCase):

    def setUp(self):
        self.engine = create_engine("sqlite:///:memory:")
        self.db_con = self.engine.connect()

    def tearDown(self):
        self.db_con.close()

    @skipIf(True, "downloads from website, maybe don't abuse it")
    def test__omie_get_files(self):
        file_list_df = pd.DataFrame({"file_name": []})
        file_list_df.to_sql("omie_historical_price", self.db_con)

        potential_missing = ["marginalpdbc_20220615.1", "marginalpdbc_20220614.1"]

        result = get_files(self.db_con, "marginalpdbc", potential_missing)

        self.assertFalse(result.empty)
