#!usr/bin/env python
# -*- coding: utf-8 -*-

import dbconfig
import pandas as pd
import requests
from json import loads
import logging
from requests.adapters import HTTPAdapter
from requests.exceptions import ConnectionError

def download_res_partner_address_from_erp(client):
    model='res.partner.address'
    data = client.model(model).search([])
    fields = ['street2', 'city', 'id_municipi', 'street', 'id', 'zip']
    data = client.model(model).read(data, fields)
    df = pd.DataFrame(data)
    #df.to_csv('res_partner_address.csv', index = False)
    return df

def download_res_municipi_from_erp(client):
    model='res.municipi'
    data = client.model(model).search([])
    data = client.model(model).read(data)
    df = pd.DataFrame(data)
    #df.to_csv('res_municipi.csv', index = False)
    return df

def get_data_zip_candidates_from_cartociudad(df):
    url = dbconfig.api_address['cartociudad_uri']
    data = {
            'no_process':'municipio,poblacion,toponimo',
            'limit':'1',
            'countrycode':'es',
            'autocancel':'true',
        }
    cartociu_adapter = HTTPAdapter(max_retries=5)
    session = requests.Session()
    session.mount(url, cartociu_adapter)
    for index, row in df.iterrows():
        data['q'] = row['street'] +' '+ row['city']
        try:
            rq = session.get(url, params=data, verify=False, timeout=5)
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
            df.loc[index, 'zip_candidate_cartociudad']
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

def get_normalized_street_address(df):
    type_street = "(?P<type_street>^CALLE\s|CARRER\s|C\/|CL\s|C\\|C|\
                            PLAZA\s|PLAÇA\s|PÇA\s|PZ|PL|\
                            AVENIDA\s|AVINGUDA\s|AVDA\s|AV\s|AVDA\s|AVD|AVD|AV\/|AV\s|AVGDA|\
                            PASSEIG\s|PASSATGE\s|PS|PG\s|PASEO\s|\
                            CRTA\s|CR\s|CARRETERA\s|\
                            RONDA\s|RDA\s)"
    street_case = "(?P<street_case>^CALLE\s|CARRER\s|C\/|CL\s|C\\|C)"
    square_case = "(?P<square_case>^PLAZA\s|PLAÇA\s|PÇA\s|PZ|PL)"
    avenue_case = "(?P<avenue_case>^AVENIDA\s|AVINGUDA\s|AVDA\s|AV\s|AVDA\s|AVD|AVD|AV\/|AV\s|AVGDA)"
    passage_case = "(?P<passage_case>^PASSEIG\s|PASSATGE\s|PS|PG\s|PASEO\s)"
    road_case = "(?P<road_case>^CRTA\s|CR\s|CARRETERA\s)"
    round_case = "(?P<round_case>^RONDA\s|RDA\s)"

    trans_dict = {
        'À': 'A','Â': 'A','Á': 'A','Ä': 'A',
        'È': 'E','Ê': 'E','É': 'E','Ë': 'E',
        'Ì': 'I','Î': 'I','Í': 'I','Ï': 'I',
        'Ò': 'O','Ô': 'O','Ó': 'O','Ö': 'O',
        'Ù': 'U','Û': 'U','Ú': 'U','Ü': 'U',
        'Ñ': 'N',
        'Ç': 'C',
        ',': ' ',
        '.': ' ',
        '-': ' ',}
    common_words = {
        "DE", "LA", "EL", "L'", "D'", "DEL","D'EN",'I','Y',"PO",
        "LOS", "LES", "LAS"
    }
    pattern = '|'.join(r"\b{}(\b|\s)".format(x) for x in common_words)
    trans_table  = str.maketrans(trans_dict)

    df['street_clean'] = df.street.str.upper()
    df['street_clean'] = df['street_clean'].str.translate(trans_table)
    df['street_clean'] = df['street_clean'].str.replace(pattern,'',)

    df['street_type_raw'] = df['street_clean'].str.extract(type_street)

    df['street_type'] = df['street_type_raw'].str.replace(street_case, 'CL', regex=True)
    df['street_type'] = df['street_type'].str.replace(square_case, 'PZ', regex=True)
    df['street_type'] = df['street_type'].str.replace(avenue_case, 'AV', regex=True)
    df['street_type'] = df['street_type'].str.replace(passage_case, 'PS', regex=True)
    df['street_type'] = df['street_type'].str.replace(road_case, 'CR', regex=True)
    df['street_type'] = df['street_type'].str.replace(round_case, 'RD', regex=True)
    df['street_type'] = df['street_type'].str.strip()

    df['street_clean'] = df['street_clean'].str.replace(type_street, '', regex=True)
    df['street_clean'] = df['street_clean'].str.replace(r'\s+', ' ', regex=True)
    df['street_clean'] = df['street_clean'].str.strip()

    street_name = "(?P<street_name>^.*?(?:(?!\D)|$))"
    df['street_name'] = df['street_clean'].str.extract(street_name)
    df['street_name'] = df['street_name'].str.strip()
    df['street_clean'] = df['street_clean'].str.replace(street_name, '', regex=True)
    df['street_clean'] = df['street_clean'].str.strip()

    df['street_number'] = df['street_clean']
    df.drop(columns=['street_clean','street_type_raw'], inplace=True)

    return df

def split_id_municipi_by_id_and_name(df):
    spec_chars = ["[","]"]
    transtab = str.maketrans(dict.fromkeys(spec_chars, ''))
    df['id_municipi_copy'] = df['id_municipi'].astype(str)
    df['id_municipi_name'] = df['id_municipi_copy'] \
        .str.translate(transtab).str.split(r"\d\,", expand=True)[1].str.strip()
    df['id_municipi_id'] = df['id_municipi_copy'] \
        .str.translate(transtab).str.split(r"\,(\s)+(\'|\")", expand=True)[0].str.strip()
    # TODO mejorar este regex para que no afecte a nombres con apostrofe
    df['id_municipi_name'] = df['id_municipi_name'].str.replace(r'\'', '', regex=True)
    df.drop(columns=['id_municipi_copy','id_municipi'], inplace=True)
    return df

def normalize_street_address(engine, client):
    df_address = download_res_partner_address_from_erp(client)
    df_municipi_normalized = split_id_municipi_by_id_and_name(df_address)
    df_address_normalized = get_normalized_street_address(df_municipi_normalized)
    with engine.begin() as connection:
        df_address_normalized.to_sql('normalized_street_address', connection, if_exists='replace', index=False)
    return df_address_normalized

# vim: ts=4 sw=4 et