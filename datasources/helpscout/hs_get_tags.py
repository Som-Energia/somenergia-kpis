from helpscout.client import HelpScout
import pandas as pd
from sqlalchemy import create_engine
from classes.alchemyClasses import HS_tag

from dbconfig import helpscout_api, local_db

def create_HS_engine():
    engine = create_engine(local_db['dbapi'])
    hs = HelpScout(app_id=helpscout_api['app_id'], app_secret=helpscout_api['app_secret'], sleep_on_rate_limit_exceeded=True)
    return hs, engine

def update_tags():

    hs, engine = create_HS_engine()

    ## Importem totes les tags de Som Energia a HelpScout
    tags = hs.tags.get()

    [[HS_tag(id=t.id, createdAt=t.createdAt, name=t.name, ticketCount=t.ticketCount)] for t in tags]

    #mirar on duplicate key update

    # tags_df = pd.DataFrame(
    #     [[t.id, t.createdAt, t.name, t.ticketCount] for t in tags],
    #     columns=['id','created_at', 'name', 'ticket_count']
    # )

    # tags_df['created_at'] = pd.to_datetime(tags_df['created_at'])

    # tags_df.to_sql('hs_tag', engine, if_exists = 'replace', index = False)

if __name__ == '__main__':
    update_tags()





