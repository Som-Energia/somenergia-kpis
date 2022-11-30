from datetime import timedelta
import datetime
from helpscout.client import HelpScout
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy import Table, Column, Integer, MetaData, DateTime
import sqlalchemy
import sys
import pendulum
import pandas as pd
import ast

def get_hs_kpis_todo(dbapi, schema = "public"):
    query = f"SELECT name, start_date, end_date, cmp_start_date, cmp_end_date, office_hours, mailbox FROM {schema}.hs_reports_descriptions"
    df = pd.read_sql(query, dbapi)
    return df

def human_string_to_date(filter):

    if filter == 'monday_of_previous_week':
        return str(datetime.datetime.today().date() - timedelta(days=datetime.datetime.today().date().weekday())+timedelta(days=0, weeks=-1))
    elif filter == 'sunday_of_previous_week':
        return str(datetime.datetime.today().date() - timedelta(days=datetime.datetime.today().date().weekday())+timedelta(days=0))
    else:
        return filter
        #raise NotImplementedError(filter)

def create_hs_engine(hs_app_id, hs_app_secret):
    hs = HelpScout(app_id=hs_app_id, app_secret=hs_app_secret, sleep_on_rate_limit_exceeded=True)
    return hs

def to_iso(dates):

    iso_dates =[]
    for date in dates:
        try:
            iso_date = pendulum.from_format(date, 'YYYY-MM-DD', tz='Europe/Madrid').in_tz('UTC').to_iso8601_string()
            iso_dates.append(iso_date)
        except ValueError:
            iso_dates.append('')
    
    return iso_dates

def calculate_kpi(hs_engine, report, dbapi, schema):
    
    start = human_string_to_date(report['start_date'])
    end = human_string_to_date(report['end_date'])
    previous_start = report['cmp_start_date']
    previous_end = report['cmp_end_date']
    office_hours = report['office_hours']
    mailbox_id = report['mailbox']
    isodates = to_iso([start, end, previous_start, previous_end])
    params = f'&mailboxes={mailbox_id}&start={isodates[0]}&end={isodates[1]}&cmpStartDate={isodates[2]}&cmpEndDate={isodates[3]}&officeHours={office_hours}'
    report_url = f'reports/email?{params}'
    report_name = report['name']
    
    print(f"Let's get report with params: {params}")

    hs_response = hs_engine.hit(report_url, 'get')
    
    df = pd.DataFrame.from_dict(hs_response[0]['current'])
    df.reset_index(inplace=True)
    df['values'] = df[['volume','resolutions','responses']].bfill(axis=1).iloc[:, 0]
    df.drop(columns=['volume', 'resolutions', 'responses'], axis=1, inplace=True)
    df['report'] = report_name
    df['calculation_time'] = pd.Timestamp.utcnow()
            
    #df['values'] = df.loc[:, df.columns != ['startDate', 'endDate']].bfill(axis=1).iloc[:, 0]
    
    return df

def get_hs_kpis(hs_engine, kpis_todo, dbapi, schema):
    engine = sqlalchemy.create_engine(dbapi)
    with engine.begin() as conn:
        for index, kpi in kpis_todo.iterrows():
            df = calculate_kpi(hs_engine, kpi, dbapi, schema)

            print(f"Let's insert report to db")
            df.to_sql(con=conn, name='hs_reports', if_exists='append', schema=schema, index=False)
            #print(f"{kpi} report inserted to db")

def update_hs_kpis_pilotatge(dbapi, schema, hs_app_id, hs_app_secret):

    todo = get_hs_kpis_todo(dbapi, schema)
    hs_engine = create_hs_engine(hs_app_id, hs_app_secret)
    get_hs_kpis(hs_engine, todo, dbapi, schema) 

if __name__ == '__main__':

    dbapi = sys.argv[1]
    schema = sys.argv[2]
    hs_app_id = sys.argv[3]
    hs_app_secret = sys.argv[4]

    update_hs_kpis_pilotatge(
        dbapi = dbapi,
        schema = schema,
        hs_app_id=hs_app_id,
        hs_app_secret=hs_app_secret
    )
