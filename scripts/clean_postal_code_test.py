#!/usr/bin/env python
# -*- coding: utf-8 -*-

from this import d
import unittest
import configdb
from erppeek import Client
import pandas as pd
from clean_postal_code import (
    download_data_from_erp,
    erp_to_pandas,
    df_to_csv,
    get_df_column_is_number,
    get_df_column_is_false,
    read_csv,
    get_df_column_is_null,
    get_df_with_null_and_false_values,
)

class ExtractDataFromErp_Test(unittest.TestCase):
    def setUp(self):
        self.client = Client(**configdb.erppeek_testing)

    def test_download_data_from_erp(self):
        data = download_data_from_erp(self.client,
        model = 'res.partner.address',
        fields = ['id', 'street', 'zip', 'id_municipi']
        )

        self.assertIsInstance(data, list)
        self.assertIsInstance(data[0], dict)
        self.assertIn('id', data[0])
        self.assertIn('zip', data[0])

    def _test_download_data_from_erp_with_limit(self):
        data = download_data_from_erp(self.client,
        model = 'res.partner.address',
        fields = ['id', 'street', 'zip', 'id_municipi'],
        limit=5)

        self.assertIsInstance(data, list)
        self.assertIsInstance(data[0], dict)
        self.assertIn('id', data[0])
        self.assertIn('zip', data[0])
        self.assertEqual(len(data), 5)

    def test_erp_to_pandas(self):
        data = download_data_from_erp(self.client,
        model = 'res.partner.address',
        fields = ['id', 'street', 'zip', 'id_municipi']
        )

        df = erp_to_pandas(data)

        self.assertIn('id', df.columns)
        self.assertIn('zip', df.columns)
        self.assertEqual(len(df), len(data))

    def test_df_to_csv(self):
        data = download_data_from_erp(self.client,
        model = 'res.partner.address',
        fields = ['name', 'zip']
        )

        df = erp_to_pandas(data)

        df_to_csv(df, 'res_partner_address.csv')

        df = read_csv('res_partner_address.csv')

        self.assertIn('name', df.columns)
        self.assertIn('zip', df.columns)
        self.assertEqual(len(df), len(data))

class LoadAndTransformData_Test(unittest.TestCase):
    def test_load_data(self):
        df = read_csv('res_partner_address_test.csv')

        name_columns = ['id', 'street', 'zip', 'id_municipi']
        self.assertListEqual(list(df.columns.values), name_columns)

    def test_when_zip_is_null(self):
        data = {
            'zip': ['12345','123', None, None],
            'id': [1,2,3,4],
        }
        df = pd.DataFrame(data)
        df = get_df_column_is_null(df, 'zip')
        self.assertEqual(len(df), 2)

    def test_when_zip_is_number(self):
        data = {
            'zip': ['12345','123', '123456', 'False'],
            'id': [1,2,3,4],
        }
        df = pd.DataFrame(data)
        df = get_df_column_is_number(df, 'zip')
        self.assertEqual(len(df), 3)

    def test_when_zip_is_false(self):
        data = {
            'zip': ['12345','False', 'False', 'False'],
            'id': [1,2,3,4],
        }
        df = pd.DataFrame(data)
        df = get_df_column_is_false(df, 'zip')
        self.assertEqual(len(df), 3)

    def test_create_df_with_null_and_false(self):
        data_raw = {
            'zip': ['12345','False', 'False', 'False', None, '123', None],
            'id': [1,2,3,4,5,6,7],
        }
        df_raw = pd.DataFrame(data_raw)

        df_raw = df_raw.set_index('id')
        data_merged_expected = {
            'zip': ['False', 'False', 'False', None, None],
            'id': [2,3,4,5,7],
        }
        df_merged_expected = pd.DataFrame(data_merged_expected)
        df_merged_expected = df_merged_expected.set_index('id')
        df_is_null_false = get_df_with_null_and_false_values(df_raw, 'zip')
        df_is_null_false.sort_values('id', inplace=True)
        df_merged_expected.sort_values('id', inplace=True)
        self.assertTrue(df_is_null_false.equals(df_merged_expected))

