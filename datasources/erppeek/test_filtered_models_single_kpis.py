import sys
from erppeek import Client
import numpy as np
import pandas as pd
import ast
import datetime
import unittest
import dbconfig
from sqlalchemy import create_engine
from sqlalchemy import text


from datasources.erppeek.filtered_models_single_kpis import (
    update_kpis,
)


class FilteredModelsERPTest(unittest.TestCase):

    def setUp(self):
        self.puppis_test = dbconfig.puppis_test_db['dbapi']

        self.freq = 'daily'

        self.erppeek = dbconfig.erppeek
        self.erp_client = Client(**self.erppeek)

        self.engine = create_engine(self.puppis_test, echo=True)
        with self.engine.connect() as con:
            with open("datasources/erppeek/pilotatge_kpis_description_setupdb.sql") as file:
                query = text(file.read())
                con.execute(query)


    def tearDown(self):
        #query = 'TRUNCATE TABLE pilotatge_int_kpis, pilotatge_float_kpis, pilotatge_kpis_description;'
        #with self.engine.connect() as con:
        #    con.execute(query)
        pass

    def test__update_kpis(self):

        result = update_kpis(self.puppis_test, self.erp_client, self.freq)


class FilteredModelsERPToDieTest(unittest.TestCase):

    def setUp(self):
        self.puppis_test = dbconfig.puppis_test_db['dbapi']

        self.freq = 'daily'

        self.erppeek = dbconfig.erppeek
        self.erp_client = Client(**self.erppeek)

        self.engine = create_engine(self.puppis_test, echo=True)

    def test__clean_kpis_db(self):

        query = 'TRUNCATE TABLE pilotatge_int_kpis, pilotatge_float_kpis, pilotatge_kpis_description;'
        with self.engine.connect() as con:
            con.execute(query)





