#!usr/bin/env python
# -*- coding: utf-8 -*-

import dbconfig
import pandas as pd
import requests
from json import loads
from requests.adapters import HTTPAdapter
from requests.exceptions import ConnectionError

def download_res_partner_data_from_erp_to_csv(client, filename,
                            model='res.partner.address', fields=[]):
    data = client.model(model).search([])
    data = client.model(model).read(data, fields)
    df = pd.DataFrame(data)
    df.to_csv(filename, index = False)
    return df

def get_data_zip_candidates_from_cartociudad(df):
    url = dbconfig.api_address['cartociudad_uri']
    data = {
            'no_process':'municipio,poblacion,toponimo',
            'limit':'1',
            'countrycode':'es',
            'autocancel':'true',
        }
    for index, row in df.iterrows():
        cartociu_adapter = HTTPAdapter(max_retries=5)
        session = requests.Session()
        data['q'] = row['street'] +' '+ row['city']
        session.mount(url, cartociu_adapter)
        try:
            rq = session.get(url, params=data, verify=False)
        except ConnectionError as e:
            print(e)
        rq.raise_for_status()
        response_text = rq.text
        startidx = response_text.find('(')
        endidx = response_text.rfind(')')
        response_list = loads(response_text[startidx + 1:endidx])
        if len(response_list) > 0:
            df.loc[index, 'zip_candidate_cartociudad'] = response_list[0]['postalCode']
        else:
            df.loc[index, 'zip_candidate_cartociudad'] = ''
    return df


def get_data_zip_candidates_from_ine(df, df_ine):
    df_ine, df = get_normalized_zips_from_ine_erp(df_ine,df)
    ine_municipio_nombre_copy = df_ine['municipio_nombre'].to_list()
    ine_codigo_postal_copy = df_ine['codigo_postal'].astype(str).to_list()

    is_in_ine = df[
            (df['id_municipi_name'].isin(ine_municipio_nombre_copy))
            &
            (df['zip'].isin(ine_codigo_postal_copy))
        ]
    df['zip_is_in_ine'] = df['id'].isin(is_in_ine['id'])
    for index, row in df.iterrows():
        row['zip_candidate_ine'] = get_zips_by_ine_municipio_nombre(df_ine, row['id_municipi_name'])
        df.loc[index, 'zip_candidate_ine'] = row['zip_candidate_ine']
    return df

def get_df_with_null_and_false_values(df,column_name):
    df_false_values = df[df[column_name] == 'False']
    df_null_values = df[df[column_name].isnull()]
    df_null_false = pd.concat([df_null_values, df_false_values])
    return df_null_false

def get_zips_by_ine_municipio_nombre(df_ine, row_id_municipi_name):
    df_grouped = df_ine.groupby(['municipio_nombre']).get_group(row_id_municipi_name)
    candidates = ' '.join(df_grouped['codigo_postal'].tolist())
    return candidates

def get_normalized_zips_from_ine_erp(df_ine,df):
    df_ine['codigo_postal'].str.replace(r'[a-zA-Z]*', '', regex=True)

    df_ine['codigo_postal'].loc[
        (df_ine['codigo_postal'].str.len()==4)] = '0' + df_ine['codigo_postal'].astype(str)

    df['zip'].loc[
        (df['zip'].str.len()==4)] = '0' + df['zip'].astype(str)
    return df_ine, df

def get_normalized_name_street_address(df):
    spec_chars = ["!",'"',"#","%","&","'","(",")",
                    "*","+",",","-",".","/",":",";","<",
                    "=",">","?","@","[","\\","]","^","_",
                    "`","{","|","}","~","-"]
    transtab = str.maketrans(dict.fromkeys(spec_chars, ''))
    df['street_clean'] = df['street'].str.replace(r'\d+', '', regex=True)
    df['street_clean'] = df['street_clean'].str.translate(transtab)
    df['street_clean'] = df['street_clean'].str.findall(r'\w{3,}').str.join(' ')
    return df

def split_id_municipi_by_id_and_name(df):
    spec_chars = ["[","]"]
    transtab = str.maketrans(dict.fromkeys(spec_chars, ''))
    df['id_municipi_copy'] = df['id_municipi']
    df['id_municipi_name'] = df['id_municipi_copy'] \
        .str.translate(transtab).str.split(r"\d\,", expand=True)[1].str.strip()
    df['id_municipi_id'] = df['id_municipi_copy'] \
        .str.translate(transtab).str.split(r"\,\s[A-Z]{1}", expand=True)[0].str.strip()
    return df

# vim: ts=4 sw=4 et