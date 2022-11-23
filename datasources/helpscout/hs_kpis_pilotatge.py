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
    table_name = 'hs_reports'
    meta = MetaData(engine)
    conv_table = Table(
        table_name,
        meta,
        Column('id', Integer, primary_key=True),
        Column('data', JSONB),
        Column('task_data_interval_start', DateTime),
        Column('task_data_interval_end', DateTime),
        Column('task_run', DateTime)
    )
    conv_table.create(engine, checkfirst=True)

    return conv_table

def to_iso(dates):
    return [pendulum.from_format(date, 'YYYY-MM-DD', tz='Europe/Madrid').in_tz('UTC').to_iso8601_string()
            for date in dates]

def get_report(hs):
    
    start='2022-11-16'
    end='2022-11-22'
    previous_start='2022-11-09'
    previous_end='2022-11-15'
    isodates = to_iso([start, end, previous_start, previous_end])
    mailbox_id=24004
    report_dades = f'reports/email?start={isodates[0]}&end={isodates[1]}&previousStart={isodates[2]}&previousEnd={isodates[3]}&mailboxid={mailbox_id}&officeHours=true'
    hs.hit(report_dades, 'get')

def get_kpis_pilotatge(hs, engine, reports_table, inici, fi):

    mailbox=123456

    params = f"mailboxes={mailbox}"

    report_url = f'reports/email?{params}'

    print(f"Let's get report with params: {params}")

    conversations = hs.hit(report_url, )

    print(f"Let's insert conversations")

    for c in conversations:
        statement = reports_table.insert().values(
            data=c.__dict__,
            task_data_interval_start=pendulum.parse(inici),
            task_data_interval_end=pendulum.parse(fi),
            task_run=pendulum.now()
        )
        engine.execute(statement)

    return True

def update_hs_kpis_pilotatge(date_interval_start, date_interval_end, engine, hs_app_id, hs_app_secret):

    date_start=pendulum.parse(date_interval_start)
    date_end=pendulum.parse(date_interval_end)

    hs, engine = create_HS_engine(engine, hs_app_id, hs_app_secret)
    reports_table = create_table(engine)

    params = {
        'inici': date_start,
        'fi': date_end,
        'status':'closed',
    }

    return get_kpis_pilotatge(hs, engine, reports_table, **params)

if __name__ == '__main__':

    date_interval_start = sys.argv[1]
    date_interval_end = sys.argv[2]

    engine = create_engine(sys.argv[3])
    hs_app_id = sys.args[4]
    hs_app_secret = sys.args[5]

    update_hs_kpis_pilotatge(
        date_interval_start=date_interval_start,
        date_interval_end=date_interval_end,
        engine=engine,
        hs_app_id=hs_app_id,
        hs_app_secret=hs_app_secret
    )
