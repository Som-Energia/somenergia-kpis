#!usr/bin/env python
# -*- coding: utf-8 -*-

import pandas as pd

def download_data_from_erp(client, **kwargs):
    data = client.model(kwargs['model']).search([], **kwargs)
    return client.model(kwargs['model']).read(data, **kwargs)

def erp_to_pandas(erp_data):
    df = pd.DataFrame(erp_data)
    return df

def df_to_csv(df, file_name):
    df.to_csv(file_name, index = False)

def read_csv(file_name, **kwargs):
    df = pd.read_csv(file_name, **kwargs)
    return df

def get_df_column_is_null(df, column_name):
    # Values for null are False, review this in erp
    #df.replace('False', None, inplace=True)
    df = df[df[column_name].isnull()]
    return df

def get_df_column_is_number(df, column_name):
    df = df[df[column_name].str.isnumeric()]
    return df

def get_df_column_is_false(df, column_name):
    df = df[df[column_name] == 'False']
    return df

def get_df_with_null_and_false_values(df,column_name):
    df_null = get_df_column_is_null(df, column_name)
    df_false = get_df_column_is_false(df, column_name)
    df_null_false = pd.concat([df_null, df_false])
    return df_null_false