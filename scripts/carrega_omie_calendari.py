#!/usr/bin/env python

import sys
import dbconfig
import pandas as pd
import datetime

from common.db_utils import create_engine

if __name__ == '__main__':

    engine = create_engine(dbconfig.local_db['dbapi'])

    to_parse = ['Día','Fecha Liquidación','Fecha Reclamaciones','Fecha Facturación','Fecha Publicación Nota Agregada','Fecha Pagos','Fecha Cobros']

    df = pd.read_csv('data/calendari_pagaments_omie.csv', parse_dates=to_parse)

    for col in to_parse:
        df[col] = df[col].dt.tz_localize('Europe/Madrid')

    df.columns = df.columns.str.replace(' ','_').str.replace('ó','o').str.replace('í','i')
    df.columns = df.columns.str.lower()

    df['request_time'] = datetime.datetime.now(datetime.timezone.utc)

    df.to_sql('omie_payments_calendar', con=engine, if_exists='replace', index=False)



# exec: python -m scripts.carrega_omie_calendari