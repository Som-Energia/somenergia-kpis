import pandas as pd
import sqlalchemy
from sqlalchemy import MetaData, Integer, DateTime, Boolean, Column, String, Table
from helpscout.client import HelpScout
import typer
import logging
from common.utils import to_iso

app = typer.Typer()

def create_table(conn, schema):

    table_name = f'hs_queries'

    meta = MetaData(conn)
    dbtable = Table(table_name, meta,
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

    dbtable.create(conn, checkfirst=True)

    import ipdb; ipdb.set_trace()
    dbtable.insert().values()



def get_hs_kpis_todo(dbapi, schema = "public"):
    query = f"SELECT name, description, action, query, frequency, is_active FROM {schema}.hs_queries WHERE is_active = true"
    df = pd.read_sql(query, dbapi)
    return df

def create_hs_engine(hs_app_id, hs_app_secret):
    return HelpScout(app_id=hs_app_id, app_secret=hs_app_secret, sleep_on_rate_limit_exceeded=True)

def get_mailbox_status(dbapi, schema, hs_engine, kpis_todo, iso_date_start: str, iso_date_end: str):
    import ipdb; ipdb.set_trace()

    params = f"{kpis_todo['query']}&start={iso_date_start}&end={iso_date_end}"

    logging.info(f"Get conversation stats with params: {params}")

    hs_response = hs_engine.conversations.get(params=params)

    logging.info(f"Insert report to db")

    df = pd.DataFrame.from_dict(hs_response[0]['current'])
    df.reset_index(inplace=True)

    df.to_sql(con=dbapi, name='hs_reports', if_exists='append', schema=schema, index=False)
    #print(f"{kpi} report inserted to db")

    return hs_response


def get_report(dbapi, schema, hs_engine, kpis_todo, iso_date_start: str, iso_date_end: str):

    # params = f'&mailboxes={mailbox_id}&start={isodates[0]}&end={isodates[1]}&cmpStartDate={isodates[2]}&cmpEndDate={isodates[3]}&officeHours={office_hours}'

    params = kpis_todo['query']
    report_url = f'reports/email?{params}&start={iso_date_start}&end={iso_date_end}'

    logging.info(f"Get report with params: {report_url}")

    hs_response = hs_engine.hit(report_url, 'get')

    df = pd.DataFrame.from_dict(hs_response[0]['current'])
    df.reset_index(inplace=True)
    df['values'] = df[['volume','resolutions','responses']].bfill(axis=1).iloc[:, 0]
    df.drop(columns=['volume', 'resolutions', 'responses'], axis=1, inplace=True)

    df['query_name'] =  kpis_todo['name']
    df['query_description'] =  kpis_todo['description']

    df['calculation_time'] = pd.Timestamp.utcnow()

    logging.info(f"Insert report to db")

    df.to_sql(con=dbapi, name='hs_reports', if_exists='append', schema=schema, index=False)
    #print(f"{kpi} report inserted to db")

@app.command()
def update_hs_kpis_pilotatge(
        interval_start: str,
        interval_end: str,
        dbapi: str,
        schema: str,
        hs_app_id: str,
        hs_app_secret: str,
        kpi_todo: str
    ):

    interval_start = to_iso(interval_start)
    interval_end = to_iso(interval_end)

    kpis_todo = get_hs_kpis_todo(dbapi, schema)

    hs_engine = create_hs_engine(hs_app_id, hs_app_secret)

    mailboxes = [
        {
            'id':m.id,
            'created_at': m.createdAt,
            'email': m.email,
            'name': m.name,
            'updated_at': m.updatedAt
        } for m in hs_engine.mailboxes.get()
    ]

    for _, kpi_todo in kpis_todo.iterrows():

        if kpi_todo['action'] == 'report':
            # if not 'mailbox' in kpi_todo['query']
            #   for mailbox in mailboxes
            #        kpi_todo['query'] = f"{kpi_todo['query']}&mailbox={mailbox['id']}"
            #        get_report()
            get_report(dbapi, schema, hs_engine, kpi_todo, dbapi, schema, interval_start, interval_end)

        elif kpi_todo['action'] == 'unassigned':

            get_mailbox_status(dbapi, schema, hs_engine, kpi_todo, interval_start, interval_end)

        else:
            raise NotImplementedError(kpi_todo['action'])


@app.command()
def setupdb(dbapi: str, schema: str):
    db_engine = sqlalchemy.create_engine(dbapi)
    with db_engine.begin() as conn:
       create_table(conn, schema)

if __name__ == '__main__':
  app()
