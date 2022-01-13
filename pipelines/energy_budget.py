import datetime
from pathlib import Path
import pandas as pd

import logging

from dbconfig import local_db

def omie_energy_buy(engine):
    try:
        omie_energy_buy = pd.read_sql('select * from omie_energy_buy', con=engine)
    except:
        logging.error('Error on fetching latest omie_energy_buy')
        # TODO handle exceptions
        raise
    return omie_energy_buy
    
def omie_price_hour(engine):
    try:
        omie_price_hour = pd.read_sql('select * from omie_price_hour', con=engine)
    except:
        logging.error('Error on fetching latest omie_price_hour')
        # TODO handle exceptions
        raise
    return omie_price_hour
    
def meff_prices_daily(engine):
    try:
        meff_prices_daily = pd.read_sql('select * from meff_prices_daily', con=engine)
    except:
        logging.error('Error on fetching latest meff_prices_daily')
        # TODO handle exceptions
        raise
    return meff_prices_daily
    
def neuro_energy_buy(engine):
    try:
        neuro_energy_buy = pd.read_sql('select * from neuro_energy_buy', con=engine)
    except:
        logging.error('Error on fetching latest neuro_energy_buy')
        # TODO handle exceptions
        raise
    return neuro_energy_buy
    
def daily_energy_budget(omie_df):
    # omie_energy_buy = omie_energy_buy or omie_energy_buy()
    pass

def hourly_energy_budget():
    pass

def interpolated_meff_prices_by_hour(meff_df):
    
    meff_df.set_index('dia')[['base_precio', 'punta_precio']]\
        .resample('h')\
        .ffill()
        #.interpolate(method='linear')

    return meff_df

def joined_timeseries(timeseries_df):
    pass

def pipe_hourly_energy_budget(engine):
    
    omie_energy_buy_df = omie_energy_buy(engine)
    omie_price_hour_df = omie_price_hour(engine)
    meff_prices_daily_df = meff_prices_daily(engine)
    neuro_energy_buy_df = neuro_energy_buy(engine)
    
    df = interpolated_meff_prices_by_hour(meff_prices_daily_df)
    #df = joined_timeseries([omie_energy_buy_df, omie_price_hour_df, meff_prices_daily_df, neuro_energy_buy_df])
    #df = pipe_daily_energy_budget(df)    

def pipe_daily_energy_budget():
    pass



# goal
# date;price;energy;source;is_forecast;budget