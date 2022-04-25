#!/usr/bin/python
# -*- coding: utf-8 -*-


helpscout_api = dict(
    app_id='',
    app_secret=''
)

local_db = dict(
    dbapi = 'postgresql://superset:superset@172.18.0.4:5432/dades'
)

test_db = dict(
    dbapi = 'postgresql://superset:superset@localhost:5432/dades_test'
)

directories = {
    'NEUROENERGIA_HISTORICAL': 'somenergia/somenergia-indicadors-kpis/realdata/NEUROENERGIA',
    'NEUROENERGIA_TEMP': 'somenergia/somenergia-indicadors-kpis/realdata/NEUROENERGIATEMP',
    'OMIE_HISTORICAL_PDBC': 'somenergia/somenergia-indicadors-kpis/realdata/OMIE/PDBC',
    'OMIE_TEMP_PDBC': 'somenergia/somenergia-indicadors-kpis/realdata/OMIETEMP/PDBC',
    'OMIE_DATA': 'somenergia/somenergia-indicadors-kpis/realdata/data'
}

test_directories = {
    'NEUROENERGIA_HISTORICAL': 'testdata/NEUROENERGIA',
    'NEUROENERGIA_TEMP': 'testdata/NEUROENERGIATEMP',
    'OMIE_HISTORICAL_PDBC': 'testdata/OMIE/PDBC',
    'OMIE_TEMP_PDBC': 'testdata/OMIETEMP/PDBC'
}

ooop_testing = dict(
    dbname = '',
    user = '',
    pwd = '',
    uri ='',
    port = ,
)

erppeek_testing = dict(
    server = ooop_testing['uri']+":"+str(ooop_testing["port"]),
    user = ooop_testing['user'],
    password = ooop_testing['pwd'],
    db = ooop_testing['dbname'],
)

api_address = {
    'cartociudad_uri' : 'https://www.cartociudad.es/geocoder/api/geocoder/candidatesJsonp?'
}
