import unittest
from unittest.case import skipIf

import pandas as pd
import numpy as np
import datetime
from pathlib import Path
import pytz

from sqlalchemy import create_engine

from datasources.neuroenergia.neuroenergia_operations import (
    shape_neuroenergia,
    neurofile_to_date
)

class NeuroenergiaOperationsTest(unittest.TestCase):

    from b2btest.b2btest import assertB2BEqual

    def setUp(self):
        self.b2bdatapath='testdata/b2bdata'

    def test__neurofile_to_date(self):
        file = Path('testdata/NEUROENERGIA/20220101_prevision-neuro.xlsx')

        request_time = neurofile_to_date(file)

        expected = datetime.datetime(2021, 12, 31, 23, 0, tzinfo=datetime.timezone.utc)
        self.assertEqual(request_time, expected)

    def test__shape_neuroenergia(self):
        create_time = datetime.datetime(2022,1,1)
        filename = 'testdata/NEUROENERGIA/20220101_prevision-neuro.xlsx'

        request_time = neurofile_to_date(Path(filename))
        neuro_df = pd.read_excel(filename)

        df = shape_neuroenergia(neuro_df, request_time, create_time)

        self.assertB2BEqual(df.to_csv(index=False))

    def test__shape_neuroenergia__correct_types(self):
        create_time = datetime.datetime(2022,1,1,tzinfo=datetime.timezone.utc)
        filename = 'testdata/NEUROENERGIA/20220101_prevision-neuro.xlsx'

        request_time = neurofile_to_date(Path(filename))
        neuro_df = pd.read_excel(filename)

        df = shape_neuroenergia(neuro_df, request_time, create_time)

        expected = {
            'date': pd.DatetimeTZDtype('ns', 'Europe/Madrid'),
            'base': np.float64,
            'market': np.float64,
            'request_time': pd.DatetimeTZDtype('ns', 'UTC'),
            'create_time': pd.DatetimeTZDtype('ns', 'UTC'),
        }

        self.assertDictEqual(df.dtypes.to_dict(), expected)
