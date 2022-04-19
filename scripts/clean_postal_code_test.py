#!/usr/bin/env python
# -*- coding: utf-8 -*-

import dbconfig
import pandas as pd
import unittest
from clean_postal_code import (
    get_df_with_null_and_false_values,
    get_data_zip_candidates_from_ine,
    download_res_partner_data_from_erp_to_csv,
    get_data_zip_candidates_from_cartociudad,
    split_id_municipi_by_id_and_name_in_separated_columns
)
from erppeek import Client

class ExtractDataFromErp_Test(unittest.TestCase):

    def setUp(self):
        self.client = Client(**dbconfig.erppeek_testing)

    def test__download_res_partner_data_from_erp_to_csv(self):
        df = download_res_partner_data_from_erp_to_csv(self.client,
        model = 'res.partner.address',
        filename='res_partner_address.csv',)
        df = pd.read_csv('res_partner_address.csv')
        df_columns_list = list(df.columns)
        fields = ['street2', 'city', 'id_municipi', 'street', 'id', 'zip']
        self.assertListEqual(
                df_columns_list,
                fields
            )

class LoadAndTransformData_Test(unittest.TestCase):

    def test__get_data_zip_candidates_from_cartociudad__from_dataframe_multiple_requests(self):
        data = {
            'id': [1,2,3],
            'street': ['Pic Peguera',
                       'Sant Jaume',
                       'Pau Casals',],
            'city': ['Girona', 'Maó', 'Girona'],
            }
        data_expect = {
            'id': [1,2,3],
            'street': ['Pic Peguera',
                       'Sant Jaume',
                       'Pau Casals',],
            'city': ['Girona', 'Maó', 'Girona'],
            'zip_candidate_cartociudad': [
                '17003 17241 17242',
                '07701',
                '17001 17005'],
        }
        df = pd.DataFrame(data)
        df_expect = pd.DataFrame(data_expect)
        responseq = get_data_zip_candidates_from_cartociudad(df)
        df_result = pd.DataFrame(responseq)
        pd.testing.assert_frame_equal(df_expect, df_result)

    def test__split_id_municipi_by_id_and_name_in_separated_columns(self):
        data = {
            'id': [1],
            'id_municipi': ["[5237, Sant Quirze del Vallès]"],
        }
        df = pd.DataFrame(data)
        df = split_id_municipi_by_id_and_name_in_separated_columns(df)
        result = df.iloc[0]
        self.assertEqual(result['id_municipi_id'], '5237')
        self.assertEqual(result['id_municipi_name'], 'Sant Quirze del Vallès')

    def test__get_df_zip_candidates_from_ine__when_zip_exist(self):
        data_raw = {
            'id': [32, 108, 35],
            'id_municipi_name': ["Sort", "Caldes de Malavella", "Castellbisbal"],
            'zip':["25560","17455", "08755"]
        }
        df_raw = pd.DataFrame(data_raw)
        data_ine = {
            'codigo_postal': [
                "25560", "25567", "25568", "25569",
                "17455", "17456",
                "8755",],
            'municipio_nombre': [
                "Sort","Sort", "Sort", "Sort",
                "Caldes de Malavella", "Caldes de Malavella",
                "Castellbisbal"],
            'municipio_id': [
                25209, 25209, 25209, 25209,
                17033, 17033,
                8054],
        }
        df_ine = pd.DataFrame(data_ine)
        data_expected = {
            'zip_is_in_ine': [
                True, True, False],
        }
        df_expected = pd.DataFrame(data_expected)

        df_result = get_data_zip_candidates_from_ine(df_raw, df_ine)
        pd.testing.assert_series_equal(
            df_expected['zip_is_in_ine'], df_result['zip_is_in_ine'])

    def test__get_df_zip_candidates_from_ine__when_zip_is_null(self):
        data_raw = {
            'id': [32, 108, 35],
            'id_municipi_name': ["Sort", "Caldes de Malavella", "Castellbisbal"],
            'zip':["","", ""]
        }
        df_raw = pd.DataFrame(data_raw)
        data_ine = {
            'codigo_postal': [
                "25560", "25567", "25568", "25569",
                "17455", "17456",
                "8755",],
            'municipio_nombre': [
                "Sort","Sort", "Sort", "Sort",
                "Caldes de Malavella", "Caldes de Malavella",
                "Castellbisbal"],
            'municipio_id': [
                25209, 25209, 25209, 25209,
                17033, 17033,
                8054],
        }
        df_ine = pd.DataFrame(data_ine)

        data_expected = {
            'zip_candidate_ine': [
                "25560 25567 25568 25569",
                "17455 17456",
                "8755",],
        }

        df_expected = pd.DataFrame(data_expected)

        df_result = get_data_zip_candidates_from_ine(df_raw, df_ine)
        pd.testing.assert_series_equal(
            df_expected['zip_candidate_ine'], df_result['zip_candidate_ine'])

    def test__get_df_with_null_and_false_values(self):
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

# vim: ts=4 sw=4 et