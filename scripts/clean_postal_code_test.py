#!/usr/bin/env python
# -*- coding: utf-8 -*-

import unittest
import configdb
from erppeek import Client
from clean_postal_code import (
    download_data_from_erp,
    erp_to_pandas,
    df_to_csv,
    read_csv,
)


class ExtractDataFromErp_Test(unittest.TestCase):
    def setUp(self):
        self.client = Client(**configdb.erppeek_testing)

    def test_download_data_from_erp(self):
        data = download_data_from_erp(self.client,
        model = 'res.partner.address',
        fields = ['name', 'zip'])

        self.assertIsInstance(data, list)
        self.assertIsInstance(data[0], dict)
        self.assertIn('name', data[0])
        self.assertIn('zip', data[0])

    def test_download_data_from_erp_with_limit(self):
        data = download_data_from_erp(self.client,
        model = 'res.partner.address',
        fields = ['name', 'zip'],
        limit=5)

        self.assertIsInstance(data, list)
        self.assertIsInstance(data[0], dict)
        self.assertIn('name', data[0])
        self.assertIn('zip', data[0])
        self.assertEqual(len(data), 5)

    def test_erp_to_pandas(self):
        data = download_data_from_erp(self.client,
        model = 'res.partner.address',
        fields = ['name', 'zip'])

        df = erp_to_pandas(data)

        self.assertIn('name', df.columns)
        self.assertIn('zip', df.columns)
        self.assertEqual(len(df), len(data))

    def test_df_to_csv(self):
        data = download_data_from_erp(self.client,
        model = 'res.partner.address',
        fields = ['name', 'zip'])

        df = erp_to_pandas(data)

        df_to_csv(df, 'res_partner.csv')

        df = read_csv('res_partner.csv')

        self.assertIn('name', df.columns)
        self.assertIn('zip', df.columns)
        self.assertEqual(len(df), len(data))