from helpscout.client import HelpScout
import pandas as pd
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy import Table, Column, Integer, MetaData
import datetime
import json

def create_HS_engine(engine, hs_app_id, hs_app_secret):
    hs = HelpScout(app_id=hs_app_id, app_secret=hs_app_secret, sleep_on_rate_limit_exceeded=True)
    return hs, engine

def create_table(engine):
    table_name = 'hs_conversation'
    meta = MetaData(engine)
    conv_table = Table(
        table_name,
        meta,
        Column('id', Integer, primary_key=True),
        Column('data', JSONB)
    )
    conv_table.create(engine, checkfirst=True)

    return conv_table

def get_conversations(hs, engine, conv_table, inici, fi, status):

    params = f"query=(modifiedAt:[{inici} TO {fi}])&status={status}"

    print(f"Let's get conversations with params: {params}")

    conversations = hs.conversations.get(params=params)

    print(f"Let's insert conversations")

    for c in conversations:
        statement = conv_table.insert().values(data=c.__dict__)
        engine.execute(statement)

    return True

def update_hs_conversations(engine, hs_app_id, hs_app_secret, verbose=2, dry_run=False, inici=None, fi=None):
    hs, engine = create_HS_engine(engine, hs_app_id, hs_app_secret)
    conv_table = create_table(engine)

    params = {
        'inici':"2022-03-14T08:00:00Z",
        'fi':"2022-03-14T8:59:59Z",
        'status':'closed',
    }

    return get_conversations(hs, engine, conv_table, **params)