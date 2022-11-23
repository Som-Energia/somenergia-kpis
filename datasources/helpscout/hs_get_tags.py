from helpscout.client import HelpScout
import pandas as pd
from sqlalchemy import create_engine
import sys
import pendulum
from sqlalchemy.orm import Session

from classes.models import HS_tag

def create_HS_engine(hs_app_id, hs_app_secret):
    hs = HelpScout(app_id=hs_app_id, app_secret=hs_app_secret, sleep_on_rate_limit_exceeded=True)
    return hs

def update_tags(engine, hs_app_id, hs_app_secret, dis, die):

    hs = create_HS_engine(hs_app_id, hs_app_secret)
    #Importem totes les tags de Som Energia a HelpScout
    tags = hs.tags.get()

    #per tema idempotencia nomes les mes noves de l'ultima execucio
    tags_insert = [HS_tag(id=t.id, name=t.name) for t in tags if dis < pendulum.parse(t.createdAt) and pendulum.parse(t.createdAt) <= die]
    print(f"insertem {len(tags_insert)} tags")
    with Session(engine) as session:
        with session.begin():
            session.add_all(tags_insert)

if __name__ == '__main__':
    args = sys.argv[1:]
    dis = pendulum.parse(args[0])
    die = pendulum.parse(args[1])
    engine = create_engine(args[2])
    hs_app_id = args[3]
    hs_app_secret = args[4]

    update_tags(engine, hs_app_id, hs_app_secret, dis, die)
