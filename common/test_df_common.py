import unittest
import datetime
import pandas as pd
from pandas.testing import assert_frame_equal
from io import StringIO
from common.df_common import basic_shape

class DFCommonTest(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def sample_df(self):
        df = pd.DataFrame({
            'year': [2021,2021,2021,2021,2021],
            'month': [6,6,6,6,6],
            'day': [10, 11, 12, 13, None],
            'value': [123, 345, 768, 901, None],
            'res': [1,2,3,4,5]
        })
        return df

    def sample_data(self):
        basic_data = StringIO("PDBC;\n2022;01;03;1;-54.7;\n2022;01;03;2;-47.3;\n*")
        return basic_data

    def sample_df_from_data(self):
        return pd.read_csv(self.sample_data(), sep = ';')


    # TODO figure out if the + 1h that we put on basic_shape is correct
    # provokes the 2022,1,3,0 instead on 2022,1,3,23 below
    def test__basic_shape(self):
        df = self.sample_df_from_data()
        noms_columnes = ['year', 'month', 'day', 'hour', 'value']
        result_df = basic_shape(df, noms_columnes)

        data = [
            [2022,1,3,1.0,-54.7, datetime.datetime(2022,1,2,23,tzinfo=datetime.timezone.utc)],
            [2022,1,3,2.0,-47.3, datetime.datetime(2022,1,3,0,tzinfo=datetime.timezone.utc)]
        ]
        noms_columnes.append('date')
        expected_df = pd.DataFrame(data, columns=noms_columnes)

        assert_frame_equal(result_df, expected_df)