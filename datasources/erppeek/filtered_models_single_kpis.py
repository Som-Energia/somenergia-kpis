import sys
from erppeek import Client
import numpy as np
import pandas as pd
import ast

# taules todo
# pilotatge_kpis_description: id, name, description, filter, erp_model, field, function, freq, type_value, teams, create_date
# 1, 'Número de factures pendents', 'num factures amb deute',  'type = out_invoice AND invoice_id.pending_state.weight > 0 AND data_venciment < today()', 'giscedata.facturacio.factura', '', 'count', 'daily', 0, 'Cobraments', '2022-08-23'

# pilotatge_numeric_kpis: kpi_id, value, create_date
# 1, 80, '2022-08-24 02:00:00'

def get_kpis_todo(dbapi, freq):
    query = f"SELECT id, name, filter, erp_model, field, function, freq, type_value FROM pilotatge_kpis_description where freq = '{freq}'"
    df = pd.read_sql(query, dbapi)
    return df

def calculate_kpi(erp_client, kpi):
    #import ipdb; ipdb.set_trace()
    obj = erp_client.model(kpi['erp_model'])
    
    filtered = obj.search(ast.literal_eval(kpi['filter']))

    if kpi['function'] == 'sum':
        raw_values = obj.read(filtered, kpi['field'])
        value = round(np.sum(raw_values),4)
    elif kpi['function'] == 'count':
        value = len(filtered)
    else:
        raise ValueError(f"kpi: {kpi['name']} id: {kpi['id']} Unknown function {kpi['function']}")
    print(f"Kpi calculated: {kpi['name']}")
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
        password=erp_password,
    )

    erp_client = Client(**erppeek) # fix this, is not unique paramater

    update_kpis(dbapi, erp_client, freq)
