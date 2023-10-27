import pandas as pd
from sqlalchemy import create_engine
import sys
from stdnum import es

def id_type_relex(id):
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

    df.loc[df['country_code'] == 'ES','id_type'] = df.loc[df['country_code'] == 'ES','id'].apply(lambda x: id_type_relex(x))
    df['is_enterprise_relex'] = np.where(df['id_type'] == 'CIF', True, False)
    df['diff'] = df['is_enterprise_relex'] != df['is_enterprise_stdnum']

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

