from datetime import timedelta
from helpscout.client import HelpScout
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy import Table, Column, Integer, MetaData, DateTime, String, ForeignKey
from sqlalchemy import create_engine, text
from sqlalchemy.sql import select
import sys
import pendulum

from sqlalchemy.ext.declarative import declarative_base
Base = declarative_base()

from sqlalchemy.orm import relationship
from sqlalchemy.orm import Session

# def create_table(engine):
#     table_name = 'hs_clean_conversations'
#     meta = MetaData(engine)
#     conv_table = Table(
#         table_name,
#         meta,
#         Column('id', Integer, primary_key=True),
#         Column('number', Integer),
#         Column('id_helpscout', Integer),
#         Column('threads', Integer),
#         Column('type', String), #tots son email?
#         Column('folderId', Integer),
#         Column('status', String),
#         Column('state', String),
#         Column('subject', String),
#         Column('mailboxId', Integer),
#         Column('createdAt', DateTime),
#         Column('closedBy', Integer),
#         Column('closedAt', DateTime),
#         Column('userUpdatedAt', DateTime),
#         Column('tags', String), #array de json?
#         Column('cc', String), #array de string
#         Column('bcc', String), #array de string?
#         Column('createdBy_id', Integer),
#         Column('createdBy_email', String), #pel createdby (client) potser millor
#         Column('closedByUser_email',String ), #del created i closeb by tambe s'ha de pensar quins fan talta fotos no, ?
#         Column('customerWaitingSince_time', DateTime),
#         Column('source_type', String),
#         Column('source_via', String), #no se ben be que ens aporten els sources
#         Column('primaryCustomer_id', Integer), #és el mateix que el creted by
#         Column('primaryCustomer_email', String), #del customer tambe amb que ens quedem
#         Column('assignee_id',Integer),
#         Column('assignee_email',String), #de qui s'ha assginat potser podem retallar
#         Column('task_data_interval_start', DateTime),
#         Column('task_data_interval_end', DateTime),
#     )
#     conv_table.create(engine, checkfirst=True)

#     return conv_table

class HS_clean_conversation(Base):
    __tablename__ = 'hs_clean_conversation'
    id = Column(Integer, primary_key=True)
    number = Column(Integer)
    id_helpscout = Column(Integer)
    threads = Column(Integer)
    type = Column(String) #tots son email?
    folderId = Column(Integer)
    status = Column(String)
    state = Column(String)
    subject = Column(String)
    mailboxId = Column(Integer)
    createdAt = Column(DateTime)
    closedBy = Column(Integer)
    closedAt = Column(DateTime)
    userUpdatedAt = Column(DateTime)
    cc = Column(String) #array de string
    bcc = Column(String) #array de string?
    createdBy_id = Column(Integer)
    createdBy_email = Column(String) #pel createdby (client) potser millor
    closedByUser_email = Column(String) #del created i closeb by tambe s'ha de pensar quins fan talta fotos no ?
    customerWaitingSince_time = Column(DateTime)
    source_type = Column(String)
    source_via = Column(String) #no se ben be que ens aporten els sources
    primaryCustomer_id = Column(Integer) #és el mateix que el creted by
    primaryCustomer_email = Column(String) #del customer tambe amb que ens quedem
    assignee_id = Column(Integer)
    assignee_email = Column(String) #de qui s'ha assginat potser podem retallar
    task_data_interval_start = Column(DateTime)
    task_data_interval_end = Column(DateTime)
    tags = relationship('HS_tag', secondary='Conversation_tag') #array de json?

class Conversation_tag(Base):
    __tablename__ = 'conversation_tag'
    clean_conversation_id = Column(Integer, ForeignKey('HS_clean_conversation.id'), primary_key = True)
    tag_id = Column(Integer, ForeignKey('HS_tag.id'), primary_key = True)

class HS_tag(Base):
   __tablename__ = 'hs_tag'
   id = Column(Integer, primary_key = True)
   created_at = Column(DateTime)
   name = Column(String)
   ticket_count = Column(Integer)
   clean_conversations = relationship(HS_clean_conversation,secondary='Conversation_tag')

def move_conversations(engine, move_table, inici, fi):

    print(f"Let's get conversations")


    textual_sql = text("SELECT * from hs_conversation where cast(data->'status' as text) = '\"closed\"' and cast(data->'createdBy'->'email' as text) not like '%@somenergia.coop' \
                        and data->'closedByUser'->'email' != '\"none@nowhere.com\"' and task_data_interval_start = :dis and task_data_interval_end = :die")
    result = engine.execute(textual_sql,dis=inici, die=fi)

    print(f"Let's insert conversations {result.rowcount}")
    import pudb; pu.db

    # for e in result:
    #     a_id = e.data.get('assignee',{'id':0})['id']

    with Session(engine) as session:

        # conversations_insert = [HS_clean_conversation(number=e.data['number'], id_helpscout=e.data['id'], threads=e.data['threads'], type=e.data['type'], folderId=e.data['folderId'],
        #                                     status=e.data['status'], state=e.data['state'], subject=e.data.get('subject',''), mailboxId=e.data['mailboxId'], createdAt=pendulum.parse(e.data['createdAt']) ,
        #                                     closedBy=e.data['closedBy'], closedAt=pendulum.parse(e.data['closedAt']), userUpdatedAt=pendulum.parse(e.data['userUpdatedAt']),
        #                                     cc=e.data['cc'], bcc=e.data['bcc'], createdBy_id=e.data['createdBy']['id'], createdBy_email=e.data['createdBy']['email'], closedByUser_email=e.data['closedByUser']['email'],
        #                                     customerWaitingSince_time=e.data['customerWaitingSince']['time'], source_type=e.data['source']['type'], source_via=e.data['source']['via'], primaryCustomer_id=e.data['primaryCustomer']['id'],
        #                                     primaryCustomer_email=e.data['primaryCustomer']['email'], assignee_id=e.data.get('assignee',{'id':0})['id'], assignee_email=e.data.get('assignee',{'email':''})['email'],
        #                                     tags=[session.query(HS_tag).filter_by(id=t['id']) for t in e.data['tags']],
        #                                     task_data_interval_start=pendulum.parse(inici), task_data_interval_end=pendulum.parse(fi)) for e in result]

        for e in result:
            #crear el objecte
            conv = HS_clean_conversation(number=e.data['number'], id_helpscout=e.data['id'], threads=e.data['threads'], type=e.data['type'], folderId=e.data['folderId'],
                                status=e.data['status'], state=e.data['state'], subject=e.data.get('subject',''), mailboxId=e.data['mailboxId'], createdAt=pendulum.parse(e.data['createdAt']) ,
                                closedBy=e.data['closedBy'], closedAt=pendulum.parse(e.data['closedAt']), userUpdatedAt=pendulum.parse(e.data['userUpdatedAt']),
                                cc=e.data['cc'], bcc=e.data['bcc'], createdBy_id=e.data['createdBy']['id'], createdBy_email=e.data['createdBy']['email'], closedByUser_email=e.data['closedByUser']['email'],
                                customerWaitingSince_time=e.data['customerWaitingSince']['time'], source_type=e.data['source']['type'], source_via=e.data['source']['via'], primaryCustomer_id=e.data['primaryCustomer']['id'],
                                primaryCustomer_email=e.data['primaryCustomer']['email'], assignee_id=e.data.get('assignee',{'id':0})['id'], assignee_email=e.data.get('assignee',{'email':''})['email'],
                                task_data_interval_start=pendulum.parse(inici), task_data_interval_end=pendulum.parse(fi))

            #buscar els tags que te
            tags=[session.query(HS_tag).filter_by(id=t['id']) for t in e.data['tags']]
            conv.tags = tags
        #afegir els tags

        # session.bulk_save_objects(conversations_insert)
        # session.add_all(conversations_insert) #una de les 2
        session.commit()


    # for email in result:
    #     e = email.data
    #     tag_ids = [dict['id'] for dict in e.data['tags']]
    #     tag_names = [dict['tag'] for dict in e['tags']]
    #     if e['tags']:
    #         print(e['tags']['tag'])
    #     statement = move_table.insert().values(number=e['number'], id_helpscout=e['id'], threads=e['threads'], type=e['type'], folderId=e['folderId'],
    #                                     status=e['status'], state=e['state'], subject=e['subject'], mailboxId=e['mailboxId'], createdAt=pendulum.parse(e['createdAt']) ,
    #                                     closedBy=e['closedBy'], closedAt=pendulum.parse(e['closedAt']), userUpdatedAt=pendulum.parse(e['userUpdatedAt']), tags=e['tags'],
    #                                     cc=e['cc'], bcc=e['bcc'], createdBy_id=e['createdBy']['id'], createdBy_email=e['createdBy']['email'], closedByUser_email=e['closedByUser']['email'],
    #                                     customerWaitingSince_time=e['customerWaitingSince']['time'], source_type=e['source']['type'], source_via=e['source']['via'], primaryCustomer_id=e['primaryCustomer']['id'],
    #                                     primaryCustomer_email=e['primaryCustomer']['email'], assignee_id=e['assignee']['id'], assignee_email=e['assignee']['email'],
    #                                     task_data_interval_start=pendulum.parse(inici), task_data_interval_end=pendulum.parse(fi))
    #     #engine.execute(statement)
    #     s.bulk_save_objects(objects)
    #    print (email)



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
    import pudb; pu.db
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