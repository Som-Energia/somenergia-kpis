import unittest

import pandas as pd
import datetime
from pathlib import Path
from sqlalchemy import MetaData, Table
from sqlalchemy import create_engine

from common.utils import restore_files

from dbconfig import (
    test_db,
    test_directories
)

from meff.meff_operations import (
    update_closing_prices_day,
    update_closing_prices_month
)
from omie.omie_operations import (
    get_historical_hour_price,
    update_latest_hour_price,
    get_historical_energy_buy,
    update_energy_buy
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
        # all tables defined will be dropped
        omie_energy_buy = Table(
            'omie_energy_buy',
            metadata, autoload=True,
            autoload_with=self.engine
        )
        metadata.drop_all()

    def test__get_historical_energy_buy(self):

        omie_dir = Path(test_directories['OMIE_HISTORICAL_PDBC']).resolve()

        result = get_historical_energy_buy(self.engine, omie_dir, verbose=2)

        self.assertEqual(result, 0)

    def test__update_energy_buy(self):

        omie_dir = Path(test_directories['OMIE_TEMP_PDBC']).resolve()

        result = update_energy_buy(self.engine, omie_dir, verbose=2)

        # TODO move this to tearDown maybe? or catch finally ?
        restore_files(omie_dir)

        self.assertEqual(result, 0)