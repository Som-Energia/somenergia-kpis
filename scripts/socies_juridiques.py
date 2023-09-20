import pandas as pd
from sqlalchemy import create_engine
import re
import sys


def is_juridic(df):

    import ipdb; ipdb.set_trace()

    #assess country code
    df['country_code'] =  df['vat'].apply(lambda x: len(x)>=2 and  x[:2])

    #get id
    df['id'] = df['vat'].apply(lambda x: x[2:])

    #assign id type
    df.loc[df['country_code'] == 'ES','id_type'] = df.loc[df['country_code'] == 'ES','id'].apply(lambda x: id_type(x))

    #new df, just to debug
    df = df[df['id_type'] == 'CIF']

    return df

def id_type(id):
    # Patrones de expresiones regulares para NIF, CIF y NIE
    patron_nif = r'^\d{8}[A-HJ-NP-TV-Z]$'
    patron_cif = r'^[ABCDEFGHJKLMNPQRSUVW]\d{7}[0-9A-J]$'
    patron_nie = r'^[XYZ]\d{7}[A-Z]$'

    # Comprueba si el número coincide con alguno de los patrones
    if re.match(patron_nif,id):
        return 'NIF'
    elif re.match(patron_cif, id):
        return 'CIF'
    elif re.match(patron_nie, id):
        return 'NIE'
    else:
        return 'Otro'


def main_socies_juridiques(sp2_dbapi):

    engine = create_engine(sp2_dbapi)

    query = """SELECT x.create_date, rp.vat
            FROM public.somenergia_soci x
            left join res_partner rp on rp.id = x.partner_id
            where data_baixa_soci is null
            and vat is not null """

    df = pd.read_sql(query, con=engine)

    engine.dispose()

    df = is_juridic(df)

    return len(df)

if __name__ == '__main__':
    dbapi = sys.argv[1]
    main_socies_juridiques(dbapi)
