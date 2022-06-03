from datetime import timedelta
from helpscout.client import HelpScout
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy import Table, Column, Integer, MetaData, DateTime, String
from sqlalchemy import create_engine, text
from sqlalchemy.sql import select
import sys
import pendulum

def create_table(engine):
    table_name = 'hs_clean_conversations'
    meta = MetaData(engine)
    conv_table = Table(
        table_name,
        meta,
        Column('id', Integer, primary_key=True),
        Column('number', Integer),
        Column('id_helpscout', Integer),
        Column('threads', Integer),
        Column('type', String), #tots son email?
        Column('folderId', Integer),
        Column('status', String),
        Column('state', String),
        Column('subject', String),
        Column('mailboxId', Integer),
        Column('createdAt', DateTime),
        Column('closedBy', Integer),
        Column('closedAt', DateTime),
        Column('userUpdatedAt', DateTime),
        Column('tags', String), #array de json?
        Column('cc', String), #array de string
        Column('bcc', String), #array de string?
        Column('createdBy_id', Integer),
        Column('createdBy_email', String), #pel createdby (client) potser millor
        Column('closedByUser_email',String ), #del created i closeb by tambe s'ha de pensar quins fan talta fotos no, ?
        Column('customerWaitingSince_time', DateTime),
        Column('source_type', String),
        Column('source_via', String), #no se ben be que ens aporten els sources
        Column('primaryCustomer_id', Integer), #és el mateix que el creted by
        Column('primaryCustomer_email', String), #del customer tambe amb que ens quedem
        Column('assignee_id',Integer),
        Column('assignee_email',String), #de qui s'ha assginat potser podem retallar
        Column('task_data_interval_start', DateTime),
        Column('task_data_interval_end', DateTime),
    )
    conv_table.create(engine, checkfirst=True)

    return conv_table

def move_conversations(engine, move_table, inici, fi):

    print(f"Let's get conversations")


    textual_sql = text("SELECT * from hs_conversation where cast(data->'status' as text) = '\"closed\"' and cast(data->'createdBy'->'email' as text) not like '%@somenergia.coop' \
                        and data->'closedByUser'->'email' != '\"none@nowhere.com\"' and task_data_interval_start = :dis and task_data_interval_end =      :die")
    result = engine.execute(textual_sql,dis=inici, die=fi)

    print(f"Let's insert conversations {result.rowcount}")
    import pudb; pu.db
    for email in result:
        e = email.data
        tag_ids = [dict['id'] for dict in e['tags']]
        tag_names = [dict['tag'] for dict in e['tags']]
        if e['tags']:
            print(e['tags']['tag'])
        statement = move_table.insert().values(number=e['number'], id_helpscout=e['id'], threads=e['threads'], type=e['type'], folderId=e['folderId'],
                                        status=e['status'], state=e['state'], subject=e['subject'], mailboxId=e['mailboxId'], createdAt=pendulum.parse(e['createdAt']) ,
                                        closedBy=e['closedBy'], closedAt=pendulum.parse(e['closedAt']), userUpdatedAt=pendulum.parse(e['userUpdatedAt']), tags=e['tags'],
                                        cc=e['cc'], bcc=e['bcc'], createdBy_id=e['createdBy']['id'], createdBy_email=e['createdBy']['email'], closedByUser_email=e['closedByUser']['email'],
                                        customerWaitingSince_time=e['customerWaitingSince']['time'], source_type=e['source']['type'], source_via=e['source']['via'], primaryCustomer_id=e['primaryCustomer']['id'],
                                        primaryCustomer_email=e['primaryCustomer']['email'], assignee_id=e['assignee']['id'], assignee_email=e['assignee']['email'],
                                        task_data_interval_start=pendulum.parse(inici), task_data_interval_end=pendulum.parse(fi))
        #engine.execute(statement)
        print (email)



    return True

def transform_hs_conversations(verbose=2, dry_run=False, inici=None, fi=None):
    args = sys.argv[1:]
    data_interval_start=pendulum.parse(args[0])
    data_interval_end=pendulum.parse(args[1])
    #fem de la setmana anterior
    data_interval_start = data_interval_start - timedelta(days=7)
    data_interval_end = data_interval_end - timedelta(days=7)
    #tornem a passar a string
    data_interval_start =  data_interval_start.isoformat()
    data_interval_end = data_interval_end.isoformat()
    #passem a format per api helpscout
    data_interval_start = data_interval_start.split('+')[0]+'Z'
    data_interval_end = data_interval_end.split('+')[0]+'Z'

    engine = create_engine(args[2])

    move_table = None
    #move_table = create_table(engine)

    params = {
        'inici': data_interval_start,
        'fi': data_interval_end,
    }

    return move_conversations(engine, move_table, **params)

if __name__ == '__main__':
    transform_hs_conversations()