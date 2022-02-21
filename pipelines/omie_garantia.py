import datetime
from pathlib import Path
import pandas as pd
from functools import reduce

import logging

def omie_payment_calendar(engine):
    query = '''
        SELECT *
        FROM omie_payments_calendar
    '''
    try:
        omie_payments_calendar = pd.read_sql(query, con=engine)
    except:
        logging.error('Error on fetching latest omie_payments_calendar')
        # TODO handle exceptions
        raise
    
    return omie_payments_calendar


def energy_budget_hourly_isforecast(engine):
    query = '''
        SELECT 
        "date", budget, 
        CASE 
            WHEN energy_price IS NULL THEN TRUE
            ELSE FALSE
        END AS "is_forecast"
        FROM energy_budget_hourly
        WHERE budget IS NOT NULL
    '''
    try:
        energy_budget_hourly_isforecast = pd.read_sql(query, con=engine)
    except:
        logging.error('Error on fetching latest energy_budget_hourly')
        # TODO handle exceptions
        raise

    return energy_budget_hourly_isforecast


def omie_garantia_dipositada(engine):
    query = '''
        SELECT garantia
        FROM omie_diposit_garantia
        ORDER BY request_time DESC
        LIMIT 1
    '''
    try:
        omie_garantia_dipositada = pd.read_sql(query, con=engine)
    except:
        logging.error('Error on fetching latest energy_budget_hourly')
        # TODO handle exceptions
        raise

    return omie_garantia_dipositada

def diposit_esperat(calendari, budget):

    budget['date'] = budget['date'].dt.tz_convert('Europe/Madrid')
    budget = budget\
        .set_index('date', drop = False)\
        .resample('D', on='date')\
        .sum()\
        .assign(is_forecast=lambda f: f.is_forecast>0)\
        .reset_index()\
        .assign(dia=lambda p: p['date'].dt.date)\
        .assign(budget_iva=lambda p: p['budget']+(p['budget']*0.21))

    calendari['fecha_pagos'] = calendari['fecha_pagos'].dt.tz_convert('Europe/Madrid')

    es_dia_de_pago = calendari[['fecha_pagos']]\
        .drop_duplicates()\
        .sort_values(by="fecha_pagos", ascending=True)\
        .reset_index(drop=True)\
        .assign(dia=lambda p: p['fecha_pagos'].dt.date)
    
    import pdb; pdb.set_trace()
    
    df = pd.merge(left=budget, right=es_dia_de_pago, how='left', on='dia')

    # TODO: Recalcular-ho segons han informat Mercat
    # No funciona així, s'ha de fer amb un for perquè els grups se solapen

    return df

def pipe_omie_garantia(engine):

    omie_payment_calendar_df = omie_payment_calendar(engine)
    energy_budget_hourly_isforecast_df = energy_budget_hourly_isforecast(engine)
    omie_garantia_dipositada_df = omie_garantia_dipositada(engine)

    df = diposit_esperat(omie_payment_calendar_df, energy_budget_hourly_isforecast_df)

    return 0


# goal
#   data;     garantia_tipus;  garantia_valor;  nota_cargo_valor
# 2022-01-01;  dipositada;        7000000;
# 2022-01-01;  esperada;          2000000;
# 2022-01-02;  dipositada;        7000000;           3500000
# 2022-01-02;  esperada;          4000000;           3500000
# 2022-01-03;  dipositada;        7000000;
# 2022-01-03;  esperada;          6000000;