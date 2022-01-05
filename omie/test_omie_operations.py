import unittest
from unittest.case import skipIf

import pandas as pd
import datetime

from sqlalchemy import create_engine
from common.utils import dateCETstr_to_tzdt

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

    def test__shape_omie(self):

        request_time = datetime.datetime(2022,1,1)
        filename = 'testdata/MARGINALPDBC/marginalpdbc_20211213.1'

        df = shape_omie(filename, request_time)

        self.assertB2BEqual(df.to_csv(index=False))

    def test__shape_omie__leading_zero(self):

        request_time = datetime.datetime(2022,1,1, tzinfo=datetime.timezone.utc)
        filename = 'testdata/MARGINALPDBC/marginalpdbc_20220103.1'

        df = shape_omie(filename, request_time)

        self.assertB2BEqual(df.to_csv(index=False))

    def test__shape_energy_buy(self):
        request_time = dateCETstr_to_tzdt('20211213')
        create_time = datetime.datetime(2022,1,1,12, tzinfo=datetime.timezone.utc)
        filename = 'testdata/PDBC/pdbc_SOMEN_20211213.1'
        df = pd.read_csv(filename, sep = ';')

        df = shape_energy_buy(df, request_time, create_time)

        self.assertB2BEqual(df.to_csv(index=False))
