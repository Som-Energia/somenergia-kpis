from datetime import timedelta
from sqlalchemy import create_engine, text
import sys
import pendulum
from sqlalchemy.orm import Session

from classes.models import HS_tag, HS_clean_conversation


def hs_clean_conversation_from_dict(data, dict_tags, start, end):
    # TODO this method could be a model's classmethod
    tags = [dict_tags[t['id']] for t in data['tags']]
    customerWaitingSince_time = pendulum.parse(data['customerWaitingSince']['time']) if 'time' in data['customerWaitingSince'] else None
    return HS_clean_conversation(
        number=data['number'], id_helpscout=data['id'], threads=data['threads'], type=data['type'], folder_id=data['folderId'],
        status=data['status'], state=data['state'], subject=data.get('subject',''), mailbox_id=data['mailboxId'], created_at=pendulum.parse(data['createdAt']) ,
        closed_by=data['closedBy'], closed_at=pendulum.parse(data.get('closedAt',end), user_updated_at=pendulum.parse(data['userUpdatedAt']),
        cc=data['cc'], bcc=data['bcc'], created_by_id=data['createdBy']['id'], created_by_email=data['createdBy']['email'], closed_by_user_email=data['closedByUser']['email'],
        customer_waiting_since_time=customerWaitingSince_time, source_type=data['source']['type'], source_via=data['source']['via'], primary_customer_id=data['primaryCustomer']['id'],
        primary_customer_email=data['primaryCustomer'].get('email',''), assignee_id=data.get('assignee',{'id':0})['id'], assignee_email=data.get('assignee',{'email':''})['email'],
        tags=tags,
        task_data_interval_start=pendulum.parse(start), task_data_interval_end=pendulum.parse(end)
    )

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
            conversations_insert = [hs_clean_conversation_from_dict(e.data, dict_tags, inici, fi) for e in result]
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
