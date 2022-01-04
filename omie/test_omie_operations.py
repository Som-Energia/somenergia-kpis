import unittest
from unittest.case import skipIf

import pandas as pd
import datetime

from sqlalchemy import create_engine

from dbconfig import local_db

from omie.omie_operations import (
    get_file_list,
    shape_omie,
    update_latest_hour_price,
    update_energy_buy,
    shape_energy_buy
)


class OmieOperationsTest(unittest.TestCase):

    from b2btest.b2btest import assertB2BEqual

    def setUp(self):
        self.b2bdatapath='testdata/b2bdata'

    @skipIf(True, "downloads from website, maybe don't abuse it")
    def test_get_file_list(self):

        hist_files = get_file_list('marginalpdbc')

        self.assertIn('Nombre', hist_files.columns)
        self.assertTrue(len(hist_files) > 0)
        self.assertEqual(hist_files['Nombre'][0][-1:],'1')

    def test_shape_omie(self):

        request_time = datetime.datetime(2022,1,1)
        filename = 'testdata/MARGINALPDBC/marginalpdbc_20211213.1'

        df = shape_omie(filename, request_time)

        self.assertB2BEqual(df.to_csv(index=False))

    def test_shape_energy_buy(self):
        request_time = datetime.datetime(2022,1,1)
        filename = 'testdata/PDBC/pdbc_SOMEN_20211213.1'
        df = pd.read_csv(filename, sep = ';')

        df = shape_energy_buy(df, request_time)

        self.assertB2BEqual(df.to_csv(index=False))
