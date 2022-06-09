import sys
import pandas as pd
from pandas.testing import assert_frame_equal

import datetime
import urllib.request, gzip
from sqlalchemy import create_engine
import unittest
import dbconfig

from datasources.meff.meff_update_closing_prices import (
    update_closing_prices,
    shape_closing_prices,
)


class MeffOperationsTest(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        # TODO destroy db
        pass

    def test__shape_closing_prices(self):

        raw_prices = pd.read_html('testdata/inputdata/meff_closing_prices.html')

        goal_prices = pd.read_html('testdata/')

        meff_df = shape_closing_prices(raw_prices)

        assert_frame_equal(meff_df, goal_prices)


@skipIf(True, "We hate dbs")
class MeffOperationsTestTestDB(unittest.TestCase):

    def setUp(self):
        self.dbapi = dbconfig.local_db['dbapi']

    def tearDown(self):
        # TODO destroy db
        pass

    def test__update_closing_prices__base(self):

        dry_run = False

        result = update_closing_prices(dbapi=self.dbapi, dry_run=dry_run)

        expected = pd.DataFrame([])

        assert_frame_equal(result, expected)