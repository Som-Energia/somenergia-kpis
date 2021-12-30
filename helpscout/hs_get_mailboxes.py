from helpscout.client import HelpScout
import pandas as pd
from sqlalchemy import create_engine

from dbconfig import helpscout_api, puppis_prod_db

def create_HS_engine():
    engine = create_engine(puppis_prod_db['dbapi'])
    hs = HelpScout(app_id=helpscout_api['app_id'], app_secret=helpscout_api['app_secret'], sleep_on_rate_limit_exceeded=True)
    return hs, engine

def update_mailboxes():

    hs, engine = create_HS_engine()

    ## Importem totes les mailboxes de Som Energia a HelpScout
    mailboxes = hs.mailboxes.get()

    mailboxes_df = pd.DataFrame(
        [[m.id, m.createdAt,m.email,m.name,m.updatedAt] for m in mailboxes],
        columns=['id','created_at','email','name','updated_at']
    )

    mailboxes_df['created_at'] = pd.to_datetime(mailboxes_df['created_at'])
    mailboxes_df['updated_at'] = pd.to_datetime(mailboxes_df['updated_at'])

    mailboxes_df.to_sql('hs_mailbox', engine, if_exists = 'replace', index = False)

if __name__ == '__main__':
    update_mailboxes()





