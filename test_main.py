import unittest

import pandas as pd
import datetime
from pathlib import Path
from sqlalchemy import text, insert, MetaData, Table, Column, Integer, String, DateTime

from sqlalchemy import create_engine

from dbconfig import test_db

from meff.meff_operations import (
    update_closing_prices_day,
    update_closing_prices_month
)
from omie.omie_operations import (
    get_historical_hour_price,
    update_latest_hour_price,
    get_historical_energy_buy
)

import datetime

class MainIntegrationTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.engine = create_engine(test_db['dbapi'])

    def setUp(self):
        # TODO use session and rollback
        pass

    def tearDown(self):
        metadata = MetaData(bind=self.engine)
        metadata.drop_all()

    def test__get_historical_energy_buy(self):

        get_historical_energy_buy(self.engine, verbose=2)
