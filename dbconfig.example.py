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
    'NEUROENERGIA_HISTORICAL': '~/somenergia/somenergia-indicadors-kpis/testdata/NEUROENERGIA',
    'NEUROENERGIA_TEMP': '~/somenergia/somenergia-indicadors-kpis/testdata/NEUROENERGIATEMP',
    'OMIE_HISTORICAL_PDBC': '~/somenergia/somenergia-indicadors-kpis/testdata/OMIE/PDBC',
    'OMIE_TEMP_PDBC': '~/somenergia/somenergia-indicadors-kpis/testdata/OMIETEMP/PDBC'
}