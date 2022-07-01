import sys
from erppeek import Client
import pendulum
import numpy as np
import pandas as pd
from sqlalchemy import create_engine

# taules todo
# pilotatge_kpis_description: id, name, description, filter, erp_model, field, function, freq, idempotent, teams, create_date
# pilotatge_numeric_kpis: kpi_id, value, create_date



def get_kpis_todo(engine, freq, idempotent):
    query = f'SELECT id, name, filter, erp_model, field, function, freq FROM pilotatge_kpis_description where freq = "{freq} and indempotent = {idempotent}"'
    df = pd.read_sql(query, engine)
    return df

def calculate_kpi(erp_client, kpi)

    obj = erp_client.model(kpi['model'])
    filtered = obj.search(kpi['filter'])

    raw_values = obj.read(filtered, kpi['field'])

    if kpi['function'] == 'sum':
        value = np.sum([t[kpi['field']] for v in raw_values])
    elif kpis['function'] == 'count':
        value = len(raw_values)
    else:
        raise ValueError(f'kpi: {kpi['name']} id: {kpi['id']} Unknown function {kpi['function']}')

    return kpi['id'], value

def get_kpis(erp_client, kpis_todo):
    kpis_values = []
    for index, kpi in kpis_todo.iterrows():
        kpis_values.append(get_kpi(erp_client, kpi))

    df = pd.DataFrame.from_records(kpis_values, columns =['kpi_id', 'value'])
    df['create_date'] = pendulum.now()

    return df

def insert_kpis(engine, df):

    df.to_sql('pilotatge_numeric_kpis', engine, if_exists = 'append', index = False)


if __name__ == '__main__':

    freq = sys.argv[1]
    idempotent = sys.argv[2]

    if freq not in ['daily','weekly'] or idempotent not in [0,1]
        raise ValueError('Unknown frequency configuration')

    erp_client = Client() # fix this, is not unique paramater
    engine = create_engine(sys.argv['0'])
    todo = get_kpis_todo(engine, freq)
    kpis = get_kpis(erp_client, todo)
    insert_kpis(engine, kpis)



