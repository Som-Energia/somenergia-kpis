import pandas as pd
import re
import pendulum
import sqlalchemy
from sqlalchemy import MetaData, Integer, DateTime, Boolean, Column, String, Table, Float
from helpscout.client import HelpScout
import typer
import logging
from common.utils import to_iso

app = typer.Typer()

class MalformedRequestException(Exception):
    pass

def create_table(conn, schema):

    table_name = 'hs_queries'

    meta = MetaData(conn)
    hs_queries_table = Table(table_name, meta,
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("create_date", DateTime(timezone=True)),
        Column("name", String),
        Column("description", String),
        Column("action", String),
        Column("query", String),
        Column("frequency", String),
        Column("is_active", Boolean),
        schema=schema
    )

    hs_queries_table.create(conn, checkfirst=True)

    conn.execute(f"TRUNCATE TABLE {schema}.{table_name}")

    pd.read_csv('datasources/helpscout/hs_queries_inserts.csv').to_sql(table_name, con=conn, schema=schema, if_exists='append', index=False)

    table_name = 'hs_queries_values'

    hs_kpis_table = Table(table_name, meta,
        Column("insert_time", DateTime(timezone=True)),
        Column("dag_start_date", DateTime(timezone=True)),
        Column("dag_end_date", DateTime(timezone=True)),
        Column("hs_query_id", Integer),
        Column("mailbox_id", Integer),
        Column("mailbox_email", String),
        Column("kpi_name", String),
        Column("value", Float),
        schema=schema
    )

    hs_kpis_table.create(meta.bind, checkfirst=True)


def get_hs_kpis_todo(dbapi, schema = "public"):
    query = f"SELECT id, name, description, action, query, frequency, is_active FROM {schema}.hs_queries WHERE is_active = true"
    df = pd.read_sql(query, dbapi)
    return df

def create_hs_engine(hs_app_id, hs_app_secret):
    return HelpScout(app_id=hs_app_id, app_secret=hs_app_secret, sleep_on_rate_limit_exceeded=True)

def get_mailbox_status(dbapi, schema, hs_engine: HelpScout, query, kpi_id: int, mailbox: dict):

    query_params = f"{query}&mailbox={mailbox['id']}" if query else f"mailbox={mailbox['id']}"

    print(f"Get conversation stats with params: {query_params}")

    hs_response = hs_engine.conversations.get(params=query_params)

    if hs_response:
        nunassigned = len(hs_response)
        oldest_unassigned_time = min([pendulum.parse(d.createdAt) for d in hs_response])
        oldest_unassigned_minutes = oldest_unassigned_time.diff(pendulum.now()).in_minutes()
        oldest_unassigned_timestamp = oldest_unassigned_time.timestamp()
    else:
        nunassigned = 0
        oldest_unassigned_timestamp = None
        oldest_unassigned_minutes = None

    print(f"Insert mailbox status query result to db ({nunassigned},{oldest_unassigned_timestamp},{oldest_unassigned_minutes})")

    df = pd.DataFrame(
        columns=['kpi_name','value'],
        data=[
            ("unassigned_conversations",nunassigned),
            ('oldest_unassigned_timestamp',oldest_unassigned_timestamp),
            ('minutes_unassigned',oldest_unassigned_minutes)
        ]
    )

    df = df.astype(
        dtype= {"kpi_name":object, "value":"float64"}
    )

    df['mailbox_id'] = mailbox['id']
    df['mailbox_email'] = mailbox['email']
    df['hs_query_id'] = kpi_id
    df['dag_start_date'] = None
    df['dag_end_date'] = None
    df['insert_time'] = pd.Timestamp.utcnow()

    df.to_sql(con=dbapi, name='hs_queries_values', if_exists='append', schema=schema, index=False)
    #print(f"{kpi} report inserted to db")

    return hs_response


def get_report(dbapi, schema, hs_engine, query, iso_date_start: str, iso_date_end: str, kpi_id: int, mailbox: dict):

    query_params = f"{query_params}&mailboxes={mailbox['id']}" if query else f"mailboxes={mailbox['id']}"

    # params = f'&mailboxes={mailbox_id}&start={isodates[0]}&end={isodates[1]}&cmpStartDate={isodates[2]}&cmpEndDate={isodates[3]}&officeHours={office_hours}'
    report_url = f'reports/email?start={iso_date_start}&end={iso_date_end}&{query_params}'

    print(f"Get report with params: {report_url}")

    hs_response = hs_engine.hit(report_url, 'get')

    df = pd.DataFrame.from_dict(hs_response[0]['current'])
    df = df.reset_index().rename(columns={'index':'kpi_name'})

    df['value'] = df[['volume','resolutions','responses']].bfill(axis=1).iloc[:, 0] # coalesce
    df.drop(columns=['volume', 'resolutions', 'responses','startDate','endDate'], axis=1, inplace=True)

    df['mailbox_id'] = mailbox['id']
    df['mailbox_email'] = mailbox['email']
    df['hs_query_id'] = kpi_id
    df['dag_start_date'] = pendulum.parse(iso_date_start)
    df['dag_end_date'] = pendulum.parse(iso_date_end)
    df['insert_time'] = pd.Timestamp.utcnow()

    print(f"Insert report to db")

    df.to_sql(con=dbapi, name='hs_queries_values', if_exists='append', schema=schema, index=False)
    print(f"{query} report inserted to db")

def get_report_or_query(action, dbapi, schema, hs_engine, query, interval_start, interval_end, kpi_id, mailbox):
    if action == 'report':
        get_report(dbapi, schema, hs_engine, query, interval_start, interval_end, kpi_id, mailbox)
    elif action == 'unassigned':
        get_mailbox_status(dbapi, schema, hs_engine, query, kpi_id, mailbox)
    else:
        raise NotImplementedError(action)

@app.command()
def update_hs_kpis_pilotatge(
        interval_start: str,
        interval_end: str,
        dbapi: str,
        schema: str,
        hs_app_id: str,
        hs_app_secret: str
):

    interval_start = to_iso(interval_start)
    interval_end = to_iso(interval_end)

    kpis_todo = get_hs_kpis_todo(dbapi, schema)

    hs_engine = create_hs_engine(hs_app_id, hs_app_secret)

    mailboxes = { m.id:
        {
            'id':m.id,
            'created_at': m.createdAt,
            'email': m.email,
            'name': m.name,
            'updated_at': m.updatedAt
        } for m in hs_engine.mailboxes.get()
    }

    for kpi_todo in kpis_todo.to_dict('records'):

        if not kpi_todo['query'] or not 'mailbox' in kpi_todo['query']:
            for mailbox_id, mailbox in mailboxes.items():
                get_report_or_query(kpi_todo['action'], dbapi, schema, hs_engine, kpi_todo['query'], interval_start, interval_end, kpi_todo['id'], mailbox)
        else:
            query_mailbox_id = re.search('mailbox(es)?=(\d*)', kpi_todo['query']).group(1)
            if not query_mailbox_id:
                raise MalformedRequestException(f"malformed query param {kpi_todo['query']}. Mailbox(es) parameter not found.")
            mailbox = mailboxes[query_mailbox_id]
            get_report_or_query(kpi_todo['action'], dbapi, schema, hs_engine, kpi_todo['query'], interval_start, interval_end, kpi_todo['id'], mailbox)

@app.command()
def setupdb(dbapi: str, schema: str):
    db_engine = sqlalchemy.create_engine(dbapi)
    with db_engine.begin() as conn:
       create_table(conn, schema)

if __name__ == '__main__':
  app()
