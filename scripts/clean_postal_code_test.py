#!/usr/bin/env python
# -*- coding: utf-8 -*-

import unittest
import configdb
from erppeek import Client
from scripts.clean_postal_code import download_data_from_erp


class ExtractDataFromErp_Test(unittest.TestCase):
    def setUp(self):
        self.client = Client(**configdb.erppeek_testing)

    def test_download_data_from_erp(self):
        data = download_data_from_erp(self.client, 'res.partner.address', ['name', 'zip'], limit=5)
        self.assertIsInstance(data, list)
        self.assertIsInstance(data[0], dict)
        self.assertIn('name', data[0])
        self.assertIn('zip', data[0])
