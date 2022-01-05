
import pandas as pd

def basic_shape(df, noms_columnes):
    df_dated = df\
        .reset_index().iloc[:,:len(noms_columnes)]\
        .set_axis(noms_columnes, axis=1)\
        .dropna()\
        .astype({'year': 'int64', 'month': 'int64', 'day': 'int64'})\
        .assign(date = lambda x: pd.to_datetime(x[['year', 'month', 'day']]))

    first_hour = df_dated['date'].dt.tz_localize('Europe/Madrid').dt.tz_convert('UTC')[0]
    last_hour = df_dated['date'].dt.tz_localize('Europe/Madrid').dt.tz_convert('UTC')[0] + pd.Timedelta(hours=len(df_dated))

    df_dated['date'] = pd.Series(pd.date_range(first_hour,last_hour,freq='H'))

    return df_dated