#!/usr/bin/env python

import sys
import dbconfig
import pandas as pd

from common.db_utils import create_engine

if __name__ == '__main__':

    engine = create_engine(dbconfig.puppis_prod_db['dbapi'])

    to_parse = ['Día','Fecha Liquidación','Fecha Reclamaciones','Fecha Facturación','Fecha Publicación Nota Agregada','Fecha Pagos','Fecha Cobros']

    df = pd.read_csv('data/calendari_pagaments_omie.csv', parse_dates=to_parse)

    df.columns = df.columns.str.replace(' ','_').str.replace('ó','o').str.replace('í','i')

    df.to_sql('omie_payments_calendar', con=engine, if_exists='replace', index=False)

