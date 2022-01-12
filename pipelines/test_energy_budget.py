import unittest

import pandas as pd
import numpy as np
import datetime
from pathlib import Path
import pytz

from energy_budget import (
    daily_energy_budget,
    hourly_energy_budget,
    interpolated_meff_prices_by_hour,
)

class NeuroenergiaOperationsTest(unittest.TestCase):
    
    from b2btest.b2btest import assertB2BEqual

    def setUp(self):
        self.b2bdatapath='testdata/b2bdata'

    def tearDown(self):
        pass

    def create_datasources(self):
        # TODO create test omie buy, prices table, meff and neuro
        pass

    def _test__interpolated_meff_prices_by_hour(self):

        request_time = datetime.datetime(2022,1,1)

        data = {
            'dia':[datetime.date(2022,1,4),datetime.date(2022,1,5)],
            'base_precio':[267.6, 290],
            'base_dif':[0,None],
            'base_dif_per':[0,0],
            'punta_precio':[278.74, 302.5],
            'punta_dif':[None,0],
            'punta_dif_per':[0,0],
            'request_time':[request_time, request_time],
        }
        meff_df = pd.DataFrame(data)

        self.assertB2BEqual(meff_df.to_csv(index=False))


    def _test__daily_energy_budget(self):
        
        self.create_datasources()

        df = daily_energy_budget()

        self.assertB2BEqual(df.to_csv(index=False))
