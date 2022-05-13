#!usr/bin/env python
# -*- coding: utf-8 -*-

from asyncio.log import logging
import dbconfig
import numpy as np
import pandas as pd
import requests
from json import loads
import logging
logging.basicConfig(level=logging.DEBUG)
from requests.adapters import HTTPAdapter
from requests.exceptions import ConnectionError

def download_res_partner_address_from_erp(client):
    model='res.partner.address'
    data = client.model(model).search([])
    fields = ['city', 'id_municipi', 'street', 'id', 'zip']
    data = client.model(model).read(data, fields)
    df = pd.DataFrame(data)
    return df

def download_res_municipi_from_erp(client):
    model='res.municipi'
    data = client.model(model).search([])
    fields = ['ine','dc','name','state','id']
    data = client.model(model).read(data, fields)
    df = pd.DataFrame(data)
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
    type_street = r"(?P<type_street>^CALLE\s|CARRER\s|C\/|CL\s|C\\|\
                            PLAZA\s|PLAÇA\s|PÇA\s|PZ\s|PL\s|\
                            AVENIDA\s|AVINGUDA\s|AVDA\s|AV\s|AVDA\s|AVD\s|AV\/|AV\s|AVGDA\s|\
                            PASSEIG\s|PASSATGE\s|PS\s|PG\s|PASEO\s|\
                            CRTA\s|CR\s|CARRETERA\s|\
                            RONDA\s|RDA\s)"
    street_case = r"(?P<street_case>^CALLE\s|CARRER\s|C\/|CL\s|C\\)"
    square_case = r"(?P<square_case>^PLAZA\s|PLAÇA\s|PÇA\s|PZ\s|PL\s)"
    avenue_case = r"(?P<avenue_case>^AVENIDA\s|AVINGUDA\s|AVDA\s|AV\s|AVDA\s|AVD\s|AV\/|AV\s|AVGDA\s)"
    passage_case = r"(?P<passage_case>^PASSEIG\s|PASSATGE\s|PS\s|PG\s|PASEO\s)"
    road_case = r"(?P<road_case>^CRTA\s|CR\s|CARRETERA\s)"
    round_case = r"(?P<round_case>^RONDA\s|RDA\s)"

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
    df['street_clean'] = df['street_clean'].str.replace(pattern,'', regex=True)

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

    street_name = r"(?P<street_name>^.*?(?:(?!\D)|$))"
    df['street_name'] = df['street_clean'].str.extract(street_name)
    df['street_name'] = df['street_name'].str.strip()
    df['street_clean'] = df['street_clean'].str.replace(street_name, '', regex=True)
    df['street_clean'] = df['street_clean'].str.strip()

    df['street_number'] = df['street_clean']
    df.drop(columns=['street_clean','street_type_raw'], inplace=True)
    return df

def split_by_id_and_name(df, name_col):
    spec_chars = ["[","]"]
    transtab = str.maketrans(dict.fromkeys(spec_chars, ''))
    df['id_copy'] = df[name_col].astype(str)
    df[name_col+'_name'] = df['id_copy'] \
        .str.translate(transtab).str.split(r"\d\,", expand=True)[1].str.strip()
    df[name_col+'_id'] = df['id_copy'] \
        .str.translate(transtab).str.split(r"\,(\s)+(\'|\")", expand=True)[0].str.strip()
    # TODO mejorar este regex para que no afecte a nombres con apostrofe
    df[name_col+'_name'] = df[name_col+'_name'].str.replace(r'\'', '', regex=True)
    df.drop(columns=['id_copy',name_col], inplace=True)
    df[name_col+'_id'] = pd.to_numeric(df[name_col+'_id'], errors='coerce').fillna(0).astype(np.int64)
    return df

def normalize_street_address(engine, client):
    logging.info("Normalizing street address")
    logging.info("Download res_partner_address from erp")
    df_address = download_res_partner_address_from_erp(client)
    logging.info("Download res_municipi from erp")
    df_municipi = download_res_municipi_from_erp(client)
    logging.info("Split by id and name in res_partner_address")
    df_address = split_by_id_and_name(df_address, 'id_municipi')
    logging.info("normalize street address")
    df_address = get_normalized_street_address(df_address)
    logging.info("Split bye id and name in res_municipi")
    df_municipi = split_by_id_and_name(df_municipi, 'state')
    logging.info("Merge res_partner_address with res_municipi")
    df_address_municipi = pd.merge(
        df_address,
        df_municipi[['ine','dc','name','state_name','id']],
        left_on='id_municipi_id', right_on='id', how='left')
    df_address_municipi.drop(columns=['id_municipi_id','id_municipi_name','id_y'], inplace=True)
    df_address_municipi.rename(columns={'id_x':'id'}, inplace=True)

    df_address_normalized = df_address_municipi[[
        'id',
        'name','state_name','city','ine','dc',
        'street','street_type','street_name','street_number','zip',
        ]]
    df_address_normalized.rename(columns={'name':'municipality'}, inplace=True)
    logging.info("Inserting normalized_street_address in database")
    with engine.begin() as connection:
        df_address_normalized.to_sql('normalized_street_address', connection,
                if_exists='replace', index=False)
    return df_address_normalized

# vim: ts=4 sw=4 et