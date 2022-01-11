import unittest

import pandas as pd
import datetime
from pathlib import Path
from sqlalchemy import text, insert, MetaData, Table, Column, Integer, String, DateTime

from sqlalchemy import create_engine

from dbconfig import test_db

from common.db_utils import (
    setup_file_table,
    insert_processed_file,
    check_processed_file,
    list_new_files,
    kpis_file_table_name
)

class DBUtilsSetupTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.engine = create_engine(test_db['dbapi'])

    def setUp(self):
        pass

    def tearDown(self):
        metadata = MetaData(bind=self.engine)
        kpis_table = Table(kpis_file_table_name, metadata, autoload_with=self.engine)
        kpis_table.drop()

    def test__setup_file_table__create(self):
        is_created = setup_file_table(self.engine)
        self.assertTrue(is_created)

    def test__setup_file_table__already_created(self):
        setup_file_table(self.engine)

        is_created = setup_file_table(self.engine)
        self.assertFalse(is_created)

class DBUtilsTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.engine = create_engine(test_db['dbapi'])

    def setUp(self):
        setup_file_table(self.engine)
        # TODO use session and rollback

    def tearDown(self):
        metadata = MetaData(bind=self.engine)
        kpis_table = Table(kpis_file_table_name, metadata, autoload_with=self.engine)
        kpis_table.drop()

    def test__dades_test_environment(self):
        pass

    def test__insert_processed_file(self):
        filename = 'deadbeef.csv'
        insert_processed_file(self.engine, filename, filetype=None)

    def test__check_file_processed__empty(self):
        exists = check_processed_file(self.engine, "deadbeef.csv")
        self.assertFalse(exists)

    def test__check_file_processed__exists(self):
        filename = 'deadbeef.csv'
        insert_processed_file(self.engine, filename, filetype=None)

        exists = check_processed_file(self.engine, filename)
        self.assertTrue(exists)

    def test__list_new_files(self):
        files = list_new_files(self.engine, 'testdata/OTHER/')
        expected = {Path('testdata/OTHER/pdbc_SOMEN_20211213.1'): False}
        self.assertDictEqual(files, expected)