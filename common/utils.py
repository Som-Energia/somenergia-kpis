import pandas as pd
import datetime
from sqlalchemy import (
    text,
    insert,
    inspect,
    MetaData,
    Table,
    Column,
    Integer,
    String,
    DateTime
)

from pathlib import Path


kpis_file_table_name = 'file_table'


def setup_file_table(engine):
    if not inspect(engine).has_table(kpis_file_table_name):
        print(f"{kpis_file_table_name} doesn't exist, let's create it")
        metadata = MetaData(engine)
        kpis_table = Table(kpis_file_table_name,metadata,
                Column('id',Integer, primary_key=True),
                Column('filename',String),
                Column('type', String),
                Column('insert_time',DateTime(timezone=True)))
        kpis_table.create()
        return True
    else:
        print(f'{kpis_file_table_name} exists. Skipping creation.')
        return False


def insert_processed_file(engine, filename, type):
    insert_time = datetime.datetime.now(datetime.timezone.utc)

    with engine.connect() as conn:
        metadata = MetaData(bind=engine)
        kpis_table = Table(kpis_file_table_name, metadata, autoload_with=engine)
        ins = kpis_table.insert().values(
            filename=filename,
            type=type,
            insert_time=insert_time)
        conn.execute(ins)


def check_processed_file(engine, filename, date_from=None):
    date_from = date_from or datetime.datetime.now() - datetime.timedelta(days=30)

    table = 'file_table'
    query = text(f"select count(*) from {table} where filename = :filename and insert_time > :date_from").bindparams(filename=filename,date_from=date_from)

    with engine.connect() as conn:
        cursor = conn.execute(query)
        count_result = cursor.fetchone()
        filefound = count_result[0] > 0

    # alternatively
    # filefound = pd.read_sql(query, engine)
    return filefound

def list_new_files(engine, directory, type=None, date_from=None):

    # create graveyard directory if it doesn't exist
    Path(f'{directory}/graveyard').mkdir(parents=False, exist_ok=True)

    flist = {p:False for p in Path(directory).iterdir() if p.is_file()}

    for f in flist.keys():
        is_logged = check_processed_file(engine, str(f), date_from)
        flist[f] = is_logged

    return flist



