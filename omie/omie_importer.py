import pandas as pd
import datetime

from sqlalchemy import create_engine


def import_marginalpdbc(filename):

    df_mpdbc = pd.read_csv(filename, sep = ';', header=None, skiprows=1, names=['year','month','day','cardinal_hour','base_price','peak_price'], index_col=False, parse_dates=[[0,1,2]])

    #TODO 25th row might be NaN, convert cardinal to hour

    return df_mpdbc