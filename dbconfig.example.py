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

TARGET = 'prod'
if TARGET == 'test':
    # Testing
    ooop = dict(
       dbname = 'somedb',
       port = 18000,
       user = 'chaplin',
       uri = 'http://lerp.somenergia.lan',
       pwd =  'werenotmachines',
    )

if TARGET == 'prod':
    # Productiu
    ooop = dict(
       dbname = 'somedb',
       port = 19000,
       user = 'groucho',
       uri = 'https://brothers.somenergia.coop',
       pwd =  'laprimeraparte',
    )

erppeek = dict(
   server='{uri}:{port}'.format(**ooop),
   db=ooop['dbname'],
   user=ooop['user'],
   password=ooop['pwd'],
)

puppis_test_db = dict(
    dbapi = 'postgresql://groucho:delasegundaparte@camarot.somenergia.coop:5432/somedb'
)
