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

def read_csv(file_name):
    df = pd.read_csv(file_name)
    return df