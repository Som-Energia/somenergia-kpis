from datetime import timedelta
from sqlalchemy import create_engine, text
import sys
import pendulum
#from ...models import  HS_tag, HS_clean_conversation
from sqlalchemy.orm import Session
from sqlalchemy.ext.declarative import declarative_base
Base = declarative_base()

import sys
try:
    # The insertion index should be 1 because index 0 is this file
    sys.path.insert(1, '/repos/somenergia-kpis/classes')  # the type of path is string #perque sempre
    # because the system path already have the absolute path to folder a
    # so it can recognize file_a.py while searching
    from models import  HS_tag, HS_clean_conversation
except (ModuleNotFoundError, ImportError) as e:
    print("{} faileure".format(type(e)))
else:
    print("Import succeeded")



def move_conversations(engine, inici, fi):

    print(f"Let's get conversations")


    textual_sql = text("SELECT * from hs_conversation where cast(data->'status' as text) = '\"closed\"' and cast(data->'createdBy'->'email' as text) not like '%@somenergia.coop' \
                        and data->'closedByUser'->'email' != '\"none@nowhere.com\"' and task_data_interval_start = :dis and task_data_interval_end = :die")
    result = engine.execute(textual_sql,dis=inici, die=fi) #dis i die han de tenir un timedelta d'una hora

    print(f"Let's insert {result.rowcount} conversations")

    with Session(engine) as session:
        with session.begin():
            tags = session.query(HS_tag)
            dict_tags = {t.id: t for t in tags}
            conversations_insert = [HS_clean_conversation(number=e.data['number'], id_helpscout=e.data['id'], threads=e.data['threads'], type=e.data['type'], folderId=e.data['folderId'],
                                                status=e.data['status'], state=e.data['state'], subject=e.data.get('subject',''), mailboxId=e.data['mailboxId'], createdAt=pendulum.parse(e.data['createdAt']) ,
                                                closedBy=e.data['closedBy'], closedAt=pendulum.parse(e.data['closedAt']), userUpdatedAt=pendulum.parse(e.data['userUpdatedAt']),
                                                cc=e.data['cc'], bcc=e.data['bcc'], createdBy_id=e.data['createdBy']['id'], createdBy_email=e.data['createdBy']['email'], closedByUser_email=e.data['closedByUser']['email'],
                                                customerWaitingSince_time=e.data['customerWaitingSince']['time'], source_type=e.data['source']['type'], source_via=e.data['source']['via'], primaryCustomer_id=e.data['primaryCustomer']['id'],
                                                primaryCustomer_email=e.data['primaryCustomer']['email'], assignee_id=e.data.get('assignee',{'id':0})['id'], assignee_email=e.data.get('assignee',{'email':''})['email'],
                                                tags=[dict_tags[t['id']] for t in e.data['tags']],
                                                task_data_interval_start=pendulum.parse(inici), task_data_interval_end=pendulum.parse(fi)) for e in result]
            session.add_all(conversations_insert)

    return True

def transform_hs_conversations(verbose=2, dry_run=False, inici=None, fi=None):
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

    move_table = None

    params = {
        'inici': data_interval_start,
        'fi': data_interval_end,
    }

    return move_conversations(engine, **params)

if __name__ == '__main__':
    transform_hs_conversations()
