import pandas as pd
from sqlalchemy import create_engine
import sys
from stdnum import es


def is_juridic(df):

    #assess country code
    df['country_code'] =  df['vat'].apply(lambda x: len(x)>=2 and  x[:2])

    #get id
    df['id'] = df['vat'].apply(lambda x: x[2:])

    #assign CIF type if country code is ES
    df.loc[df['country_code'] == 'ES','CIF'] = df.loc[df['country_code'] == 'ES','id'].apply(lambda x: es.cif.is_valid(x))

    #select just CIF members
    df = df[df['CIF'] == True]

    return df[['create_date', 'vat', 'CIF']]


def main_socies_juridiques(puppis_dbapi):

    engine = create_engine(puppis_dbapi)

    # select active members and its vat
    query = """SELECT x.create_date, rp.vat
            FROM erp.somenergia_soci x
            left join erp.res_partner rp on rp.id = x.partner_id
            where data_baixa_soci is null
            and vat is not null """

    df = pd.read_sql(query, con=engine)

    df = is_juridic(df)

    df.to_sql(name='socies_juridiques', con=engine, if_exists='replace', schema='prod', index=False)

    engine.dispose()

    return len(df)

if __name__ == '__main__':
    dbapi = sys.argv[1]
    main_socies_juridiques(dbapi)

