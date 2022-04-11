#!usr/bin/env python
# -*- coding: utf-8 -*-

def download_data_from_erp(client, model, fields, limit=10):
    data = client.model(model).search([], limit=limit, fields=fields)
    return client.model(model).read(data, fields=fields)

