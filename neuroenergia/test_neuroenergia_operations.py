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


class NeuroenergiaOperationsTest(unittest.TestCase):

    from b2btest.b2btest import assertB2BEqual

    def setUp(self):
        self.b2bdatapath='testdata/b2bdata'

    def test_shape_neuroenergia(self):
        request_time = datetime.datetime(2022,1,1)
        filename = 'testdata/NEUROENERGIA/20220101_prevision-neuro.xlsx'
        df = pd.read_excel(filename)

        # TODO

        self.assertB2BEqual(df.to_csv(index=False))
