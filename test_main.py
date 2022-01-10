import unittest

import pandas as pd
from pandas.io import sql
import datetime
from pathlib import Path
from sqlalchemy import MetaData, Table, Column, String, Integer, DateTime
from sqlalchemy import create_engine


from common.utils import restore_files, dateCETstr_to_tzdt

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
    update_energy_buy,
    update_historical_hour_price
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

        sql.execute('DROP TABLE IF EXISTS omie_energy_buy', self.engine)
        sql.execute('DROP TABLE IF EXISTS omie_price_hour', self.engine)

        # metadata = MetaData(bind=self.engine)
        # # all tables defined will be dropped
        # omie_energy_buy = Table(
        #     'omie_energy_buy',
        #     metadata, autoload=True,
        #     autoload_with=self.engine
        # )
        # # omie_price_hour = Table(
        # #     'omie_price_hour',
        # #     metadata, autoload=True,
        # #     autoload_with=self.engine
        # # )
        # metadata.drop_all()

    def create_historical(self, reading_date: str):
        metadata = MetaData(bind=self.engine)
        # all tables defined will be dropped
        omie_price_hour = Table(
            'omie_price_hour',
            metadata,
            Column('date', DateTime(timezone=True)),
            Column('price', Integer),
            Column('df_current_day_dated',DateTime(timezone=True))
        )
        omie_price_hour.create()

        with self.engine.connect() as conn:

            ins = omie_price_hour.insert().values(
                date=dateCETstr_to_tzdt(reading_date),
                price=100,
                df_current_day_dated=dateCETstr_to_tzdt(reading_date))
            conn.execute(ins)

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

    # TODO mock web call so we can always run this tests
    @unittest.skipIf(True, 'Test requires web access to omie. Also needs fixing :P')
    def test__update_historical_hour_price(self):

        self.create_historical('20220105')

        result = update_historical_hour_price(self.engine, verbose=3)

        self.assertEqual(result, 0)

    # TODO mock web call so we can always run this tests
    @unittest.skipIf(True, 'Test requires web access to omie. Also needs fixing :P')
    def test__update_historical_hour_price__already_inserted(self):

        self.create_historical('22220202')

        result = update_historical_hour_price(self.engine, verbose=3)

        self.assertEqual(result, 1)