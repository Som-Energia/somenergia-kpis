import unittest
import pandas as pd
from pandas.testing import assert_frame_equal
import datetime

from sqlalchemy import create_engine

from omie.omie_importer import import_marginalpdbc


class OmieImporterTest(unittest.TestCase):

    from b2btest.b2btest import assertB2BEqual

    def setUp(self):
        self.b2bdatapath='testdata/b2bdata'

    def test_import_marginalpdbc(self):

        filename = 'testdata/MARGINALPDBC/marginalpdbc_20211213.1'

        df = import_marginalpdbc(filename)

        self.assertB2BEqual(df.to_csv(index=False))
