import datetime
from pathlib import Path
import pandas as pd
from functools import reduce

import logging

from dbconfig import local_db

def omie_energy_buy(engine):
    try:
        omie_energy_buy = pd.read_sql('select * from omie_energy_buy', con=engine)
    except:
        logging.error('Error on fetching latest omie_energy_buy')
        # TODO handle exceptions
        raise

    omie_energy_buy = omie_energy_buy.\
        rename(columns={'demand':'energy'})
    omie_energy_buy['energy'] = -omie_energy_buy['energy']

    omie_energy_buy = omie_energy_buy[['date','energy']]

    return omie_energy_buy
    
def omie_price_hour(engine):
    try:
        omie_price_hour = pd.read_sql('select * from omie_price_hour', con=engine)
    except:
        logging.error('Error on fetching latest omie_price_hour')
        # TODO handle exceptions
        raise
    
    omie_price_hour = omie_price_hour[['date','price']]
    
    return omie_price_hour
    
def last_meff_prices_daily(engine):
    try:
        last_meff_prices_daily = pd.read_sql('select * from meff_precios_cierre_dia where request_time IN (select request_time from meff_precios_cierre_dia order by request_time desc limit 1)', con=engine)
    except:
        logging.error('Error on fetching latest meff_prices_daily')
        # TODO handle exceptions
        raise
    last_meff_prices_daily = last_meff_prices_daily.\
        rename(columns={'base_precio':'price_forecast'})

    last_meff_prices_daily = last_meff_prices_daily[['dia','price_forecast','request_time']]

    last_meff_prices_daily = last_meff_prices_daily.\
        rename(columns={'request_time':'meff_request_time'})

    return last_meff_prices_daily

def last_neuro_energy_buy(engine):
    try:
        last_neuro_energy_buy = pd.read_sql('select * from energy_buy_forecast where request_time IN (select request_time from energy_buy_forecast order by request_time desc limit 1)', con=engine)
    except:
        logging.error('Error on fetching latest neuro_energy_buy')
        # TODO handle exceptions
        raise
    
    last_neuro_energy_buy = last_neuro_energy_buy.\
        rename(columns={'base':'energy_forecast'})

    last_neuro_energy_buy = last_neuro_energy_buy[['date','energy_forecast','request_time']]

    last_neuro_energy_buy = last_neuro_energy_buy.\
        rename(columns={'request_time':'neuro_request_time'})

    return last_neuro_energy_buy
        

def interpolated_last_meff_prices_by_hour(meff_df):
    
    next_day = pd.DataFrame({'dia':[max(meff_df['dia']) + datetime.timedelta(days=1)]})
    meff_df = meff_df.append(next_day)

    meff_df['date'] = meff_df['dia'].dt.tz_localize('Europe/Madrid').dt.tz_convert('UTC')
    
    meff_df = meff_df\
        .reset_index()\
        .set_index('date', drop = False)[['price_forecast']]\
        .resample('h')\
        .interpolate(method='linear')\
        .reset_index(level=0)    

    meff_df = meff_df[meff_df['date'] != max(meff_df['date'])]
    
    return meff_df

# TODO Improve this?
# assumes input dataframes have a timezone-aware date column and that the timezone is 'Europe/Madrid'
# otherwise throws
def joined_timeseries(timeseries_dfs):
    
    from_date = min([df.iloc[0]['date'] for df in timeseries_dfs])
    to_date = max([df.iloc[-1]['date'] for df in timeseries_dfs])
    
    df_timeline = pd.DataFrame(
        pd.date_range(
            from_date.astimezone('Europe/Madrid'),
            (to_date.astimezone('Europe/Madrid') + datetime.timedelta(days=1)),
            freq = 'H',
            normalize = True,
            closed = 'left',
            tz='Europe/Madrid')
        ).\
        rename(columns={0:'date'})
    
    df_timeline['date'] = pd.to_datetime(df_timeline['date'], utc=True)

    df_merged = reduce(lambda  left,right: pd.merge(left,right,on=['date'],
                                            how='left'), [df_timeline] + timeseries_dfs)
    
    return df_merged

def hourly_energy_budget(df):

    df['energy_price'] = df['energy'] * df['price']
    df['energy_price_forecast'] = df['energy'] * df['price_forecast']
    df['energy_forecast_price'] = df['energy_forecast'] * df['price']
    df['energy_forecast_price_forecast'] = df['energy_forecast'] * df['price_forecast']

    df['budget'] = (
        df['energy_price']\
        .combine_first(df['energy_forecast_price'])\
        .combine_first(df['energy_price_forecast'])\
        .combine_first(df['energy_forecast_price_forecast'])
    )

    return df

def pipe_hourly_energy_budget(engine):
    
    omie_energy_buy_df = omie_energy_buy(engine)
    omie_price_hour_df = omie_price_hour(engine)
    meff_prices_daily_df = last_meff_prices_daily(engine)
    neuro_energy_buy_df = last_neuro_energy_buy(engine)
    meff_prices_daily_df = interpolated_last_meff_prices_by_hour(meff_prices_daily_df)

    df = joined_timeseries([omie_energy_buy_df, omie_price_hour_df, meff_prices_daily_df, neuro_energy_buy_df])
    df = hourly_energy_budget(df)

    df.to_sql('energy_budget_hourly', engine, if_exists = 'replace', index = False)

    return 0
    

# goal
# date;price;energy;source;is_forecast;budget