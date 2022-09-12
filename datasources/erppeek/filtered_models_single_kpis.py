import sys
from erppeek import Client
import numpy as np
import pandas as pd
import ast
import datetime

# taules todo
# pilotatge_kpis_description: id, name, description, filter, erp_model, , context, field, function, freq, type_value, teams, create_date
# 1, 'Número de factures pendents', 'num factures amb deute',  'type = out_invoice AND invoice_id.pending_state.weight > 0 AND data_venciment < today()', 'giscedata.facturacio.factura', '', 'count', 'daily', 0, 'Cobraments', '2022-08-23'

# pilotatge_numeric_kpis: kpi_id, value, create_date
# 1, 80, '2022-08-24 02:00:00'

def get_kpis_todo(dbapi, freq):
    query = f"SELECT id, name, filter, erp_model, context, field, function, freq, type_value FROM pilotatge_kpis_description where freq = '{freq}'"
    df = pd.read_sql(query, dbapi)
    return df

def filter_string_to_list(filter):

    filter = filter.replace('__today__', str(datetime.datetime.today().date()))
    filter = filter.replace('__7_days_ago__', str(datetime.datetime.today().date()  - datetime.timedelta(days=7)))
    filter = filter.replace('__yesterday__', str(datetime.datetime.today().date()  - datetime.timedelta(days=1)))

    #if 'lot_actual' in filter:
    #    id = erp_client.model(lot).search(obert)
    #    filter = filter.replace('lot_actual', id)

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
    df['create_date'] = pd.Timestamp.now().tz_localize(tz='Europe/Madrid')

    return df

def update_kpis(dbapi, erp_client, freq):

    todo = get_kpis_todo(dbapi, freq)
    kpis = get_kpis(erp_client, todo)

    kpis\
        .query("type_value == 'float'")\
        .drop(columns=['type_value'])\
        .to_sql('pilotatge_float_kpis', dbapi, if_exists = 'append', index = False)

    print(f"Float Kpis inserted {str(len(kpis[kpis['type_value']=='float']))} rows")

    kpis\
        .query("type_value == 'int'")\
        .drop(columns=['type_value'])\
        .to_sql('pilotatge_int_kpis', dbapi, if_exists = 'append', index = False)

    print(f"Integer Kpis inserted {str(len(kpis[kpis['type_value']=='int']))} rows")

if __name__ == '__main__':

    dbapi = sys.argv[1]
    freq = sys.argv[2]
    erp_server = sys.argv[3]
    erp_db = sys.argv[4]
    erp_user = sys.argv[5]
    erp_password = sys.argv[6]

    if freq not in ['daily','weekly']:
        raise ValueError('Unknown frequency configuration')

    erppeek = dict(
        server=erp_server,
        db=erp_db,
        user=erp_user,
        password=erp_password
    )

    erp_client = Client(**erppeek)

    update_kpis(dbapi, erp_client, freq)
