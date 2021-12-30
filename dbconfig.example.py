#!/usr/bin/python
# -*- coding: utf-8 -*-


helpscout_api = dict(
    app_id='',
    app_secret=''
)

puppis_prod_db = dict(
    dbapi = 'postgresql://blabla:blabla@puppis.somenergia.lan:5432/somenergia'
)

local_superset_db = dict(
    dbapi = 'postgresql://superset:superset@172.18.0.4:5432/dades'
)

# Used to send mails
tomatic_SMTP = dict(
    host = 'smtp.gmail.com',
    port = 587,
    user = 'tomatic@somenergia.coop',
    password = 'blabla',
)