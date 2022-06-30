from datetime import timedelta
from helpscout.client import HelpScout
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy import Table, Column, Integer, MetaData, DateTime
from sqlalchemy import create_engine
import sys
import pendulum

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
        Column('data', JSONB),
        Column('task_data_interval_start', DateTime),
        Column('task_data_interval_end', DateTime)
    )
    conv_table.create(engine, checkfirst=True)

    return conv_table

def get_conversations(hs, engine, conv_table, inici, fi, status):

    params = f"query=(modifiedAt:[{inici} TO {fi}])&status={status}"

    print(f"Let's get conversations with params: {params}")

    conversations = hs.conversations.get(params=params)

    print(f"Let's insert conversations")

    for c in conversations:
        statement = conv_table.insert().values(data=c.__dict__,task_data_interval_start=pendulum.parse(inici), task_data_interval_end=pendulum.parse(fi))
        engine.execute(statement)

    return True

def update_hs_conversations(verbose=2, dry_run=False, inici=None, fi=None):
    args = sys.argv[1:]
    data_interval_start=pendulum.parse(args[0])
    data_interval_end=pendulum.parse(args[1])
    #fem de la setmana anterior
    data_interval_start = data_interval_start - timedelta(days=7)
    data_interval_end = data_interval_end - timedelta(days=7)
    #tornem a passar a string
    data_interval_start = data_interval_start.strftime("%Y-%m-%dT%H:%M:%SZ")
    data_interval_end = data_interval_end.strftime("%Y-%m-%dT%H:%M:%SZ")

    engine = create_engine(args[2])
    hs_app_id = args[3]
    hs_app_secret = args[4]

    hs, engine = create_HS_engine(engine, hs_app_id, hs_app_secret)
    conv_table = create_table(engine)

    params = {
        'inici': data_interval_start,
        'fi': data_interval_end,
        'status':'closed',
    }

    return get_conversations(hs, engine, conv_table, **params)

if __name__ == '__main__':
    update_hs_conversations()
