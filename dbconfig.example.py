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
    'NEUROENERGIA_TEMP': 'testdata/TEMP/NEUROENERGIA',
    'OMIE_HISTORICAL_PDBC': 'testdata/PDBC',
    'OMIE_TEMP_PDBC': 'testdata/TEMP/PDBC'
}
