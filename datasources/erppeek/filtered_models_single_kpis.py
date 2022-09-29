import sys
from erppeek import Client
import numpy as np
import pandas as pd
import ast
import datetime

def get_kpis_todo(dbapi, freq, schema = "public"):
    query = f"SELECT id, name, filter, erp_model, context, field, function, freq, type_value FROM {schema}.erppeek_kpis_description where freq = '{freq}'"
    df = pd.read_sql(query, dbapi)
    return df

def filter_string_to_list(filter):

    filter = filter.replace('__today__', str(datetime.datetime.today().date()))
    filter = filter.replace('__7_days_ago__', str(datetime.datetime.today().date() - datetime.timedelta(days=7)))
    filter = filter.replace('__3_days_ago__', str(datetime.datetime.today().date() - datetime.timedelta(days=3)))
    filter = filter.replace('__yesterday__', str(datetime.datetime.today().date() - datetime.timedelta(days=1)))

    return ast.literal_eval(filter)

def calculate_kpi(erp_client, kpi):
    obj = erp_client.model(kpi['erp_model'])
    filter = filter_string_to_list(kpi['filter'])
    cxt = ast.literal_eval(kpi['context'])
    filtered = obj.search(filter, context = cxt)

    if kpi['function'] == 'sum':
        raw_values = obj.read(filtered, kpi['field'])
        value = round(np.sum(raw_values),4)
    elif kpi['function'] == 'count':
        value = len(filtered)
    else:
        raise ValueError(f"kpi: {kpi['name']} id: {kpi['id']} Unknown function {kpi['function']}")
    print(f"Kpi calculated: {kpi['id']}: {kpi['name']} - valor: {value}")
    return kpi['id'], value, kpi['type_value']

def get_kpis(erp_client, kpis_todo):
    kpis_values = []
    for index, kpi in kpis_todo.iterrows():
        kpis_values.append(calculate_kpi(erp_client, kpi))

    df = pd.DataFrame.from_records(kpis_values, columns =['kpi_id', 'value', 'type_value'])
    df['create_date'] = datetime.datetime.utcnow()
    df['create_date'] = df['create_date'].dt.tz_localize(tz='UTC')

    return df

def update_kpis(dbapi, erp_client, freq, schema= "public"):

    todo = get_kpis_todo(dbapi, freq, schema)
    kpis = get_kpis(erp_client, todo)

    kpis\
        .query("type_value == 'float'")\
        .drop(columns=['type_value'])\
        .to_sql('pilotatge_float_kpis', dbapi, schema=schema, if_exists='append', index=False)

    print(f"Float Kpis inserted {str(len(kpis[kpis['type_value']=='float']))} rows")

    kpis\
        .query("type_value == 'int'")\
        .drop(columns=['type_value'])\
        .to_sql('pilotatge_int_kpis', dbapi, schema=schema, if_exists='append', index=False)

    print(f"Integer Kpis inserted {str(len(kpis[kpis['type_value']=='int']))} rows")

if __name__ == '__main__':

    dbapi = sys.argv[1]
    freq = sys.argv[2]
    erp_server = sys.argv[3]
    erp_db = sys.argv[4]
    erp_user = sys.argv[5]
    erp_password = sys.argv[6]
    schema = sys.argv[7]

    if freq not in ['daily','weekly']:
        raise ValueError('Unknown frequency configuration')

    erppeek = dict(
        server=erp_server,
        db=erp_db,
        user=erp_user,
        password=erp_password
    )

    erp_client = Client(**erppeek)

    update_kpis(dbapi, erp_client, freq, schema)
