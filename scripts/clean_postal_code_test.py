#!/usr/bin/env python
# -*- coding: utf-8 -*-

from cmath import nan
import dbconfig
import pandas as pd
import unittest
from clean_postal_code import (
    download_res_municipi_from_erp,
    download_res_partner_address_from_erp,
    split_id_municipi_by_id_and_name,
    get_df_with_null_and_false_values,
    get_data_zip_candidates_from_ine,
    get_data_zip_candidates_from_cartociudad,
    get_normalized_street_address,
    get_zips_by_ine_municipio_nombre,
    get_normalized_zips_from_ine_erp,
    normalize_street_address,
)
from erppeek import Client
from sqlalchemy import create_engine, MetaData, Table

class NormalizedStreetAddress_Test(unittest.TestCase):

    def setUp(self):
        self.client = Client(**dbconfig.erppeek_testing)
        self.engine = create_engine(dbconfig.test_db['dbapi'])

    def _tearDown(self):
        metadata = MetaData(bind=self.engine)
        normalized_table = Table('normalized_street_address', metadata, autoload_with=self.engine)
        normalized_table.drop()

    def test__download_res_partner_address_from_erp(self):
        df = download_res_partner_address_from_erp(self.client)
        df_columns_list = list(df.columns)
        fields = ['street2', 'city', 'id_municipi', 'street', 'id', 'zip']
        self.assertCountEqual(df_columns_list,fields)

    def test__download_res_municipi_from_erp(self):
        df = download_res_municipi_from_erp(self.client)
        df_columns_list = list(df.columns)
        fields = ['id', 'imu_diputacio', 'ine',
            'subsistema_id', 'comarca', 'state', 'presentar_informe',
            'dc', 'name', 'climatic_zone']
        self.assertCountEqual(df_columns_list, fields)

    def _test__get_data_zip_candidates_from_cartociudad__from_dataframe_multiple_requests(self):
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

    def test__split_id_municipi_by_id_and_name(self):
        data = {
            'id': [1],
            'id_municipi': ["[5237, 'Sant Quirze del Vallès']"],
        }
        df = pd.DataFrame(data)
        df = split_id_municipi_by_id_and_name(df)
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
                True, True, True],
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
                "08755",],
        }

        df_expected = pd.DataFrame(data_expected)

        df_result = get_data_zip_candidates_from_ine(df_raw, df_ine)
        pd.testing.assert_series_equal(
            df_expected['zip_candidate_ine'], df_result['zip_candidate_ine'])

    def test__get_df_zip_candidates_from_ine__when_zip_start_zero(self):
        data_raw = {
            'id': [35],
            'id_municipi_name': ["Castellbisbal"],
            'zip':[""]
        }
        df_raw = pd.DataFrame(data_raw)
        data_ine = {
            'codigo_postal': ["8755",],
            'municipio_nombre': ["Castellbisbal"],
            'municipio_id': [8054],
        }
        df_ine = pd.DataFrame(data_ine)

        data_expected = {
            'zip_candidate_ine': ["08755",],
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

    def test__get_normalized_street_address(self):
        data_raw = {
            'id': [1,2],
            'street': ['Carrer de la Rambla 1 5ºE', 'Pic de Peguera 15']
        }
        df_raw = pd.DataFrame(data_raw)
        data_expected = {
            'id': [1,2],
            'street': ['Carrer de la Rambla 1 5ºE', 'Pic de Peguera 15'],
            'street_type': ['CL', nan],
            'street_name': ['RAMBLA', 'PIC PEGUERA'],
            'street_number': ['1 5ºE', '15'],
        }
        df_expected = pd.DataFrame(data_expected)
        df_result = get_normalized_street_address(df_raw)
        df_expected.set_index('id', inplace=True)
        df_result.set_index('id', inplace=True)
        pd.testing.assert_frame_equal(df_expected, df_result)

    def test__get_zip_by_ine_municipio_nombre(self):
        data_raw = {
            'id': [35],
            'id_municipi_name': ["Castellbisbal"],
            'zip':[""]
        }
        dat_raw = pd.DataFrame(data_raw)
        data_ine = {
            'codigo_postal': ["8755",],
            'municipio_nombre': ["Castellbisbal"],
            'municipio_id': [8054],
        }
        df_ine = pd.DataFrame(data_ine)
        df_ine, df = get_normalized_zips_from_ine_erp(df_ine, dat_raw)
        result = get_zips_by_ine_municipio_nombre(df_ine,'Castellbisbal')
        self.assertEqual('08755', result)

    def test__normalize_street_address(self):
        client = self.client
        engine = self.engine
        normalize_street_address(engine, client)
        query = '''
            SELECT * FROM normalized_street_address where id = 1;
        '''
        df_result = pd.read_sql_query(query, engine)
        data_expected = {
            'id': [1],
            'municipality': ['Girona'],
            'state_name': ['Girona'],
            'city':['Girona'],
            'ine': ['17079'],
            'dc': ['2'],
            'street': ['CL. Pic de Peguera, 11 A 2 8'],
            'street_type': ['CL'],
            'street_name': ['PIC PEGUERA'],
            'street_number': ['11 A 2 8'],
            'zip':['17003'],
        }
        df_expected = pd.DataFrame(data_expected)
        pd.testing.assert_frame_equal(df_expected, df_result)



# vim: ts=4 sw=4 et