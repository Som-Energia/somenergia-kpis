#!/usr/bin/env python

import pandas as pd
import datetime
from pathlib import Path
import dbconfig

from sqlalchemy import create_engine


from common.db_utils import create_engine

def update_garantia(engine, garantia_file):
    garantia_table = 'omie_diposit_garantia'

    df = pd.read_csv(garantia_file, header=None)
    df.columns = ['garantia']
    df['request_time'] = datetime.datetime.now(datetime.timezone.utc)
    df['garantia'] = float(df['garantia'])
        
    table_exist = pd.read_sql(f"SELECT EXISTS (SELECT relname FROM pg_class WHERE relname = '{garantia_table}');", con=engine)

    if table_exist.bool():

        result = pd.read_sql('select garantia from omie_diposit_garantia order by request_time desc limit 1', con=engine)
        
        if result.equals(df):
            print(f'Not inserted exists {table_exist.bool()} and df is {df.to_dict()}')
            return

    df.to_sql(f'{garantia_table}', con=engine, if_exists='append', index=False)
    return df


if __name__ == '__main__':

    engine = create_engine(dbconfig.puppis_prod_db['dbapi'])
    garantia_file = Path(dbconfig.directories['OMIE_DATA']) / 'garantia.csv'
    result = update_garantia(engine, garantia_file)
    print(result)