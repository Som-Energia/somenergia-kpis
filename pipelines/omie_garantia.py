import datetime
from pathlib import Path
import pandas as pd
from functools import reduce

import logging

def omie_payment_calendar(engine):
    try:
        omie_payments_calendar = pd.read_sql('select * from omie_payments_calendar', con=engine)
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






# goal
#   data;     garantia_tipus;  garantia_valor;  nota_cargo_valor
# 2022-01-01;  dipositada;        7000000;
# 2022-01-01;  esperada;          2000000;
# 2022-01-02;  dipositada;        7000000;           3500000
# 2022-01-02;  esperada;          4000000;           3500000
# 2022-01-03;  dipositada;        7000000;
# 2022-01-03;  esperada;          6000000;