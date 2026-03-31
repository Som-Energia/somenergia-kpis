from helpscout.client import HelpScout
from sqlalchemy import create_engine
import sys
import pendulum
from sqlalchemy.orm import Session

from classes.models import HS_mailbox

def create_HS_engine(engine, hs_app_id, hs_app_secret):
    hs = HelpScout(app_id=hs_app_id, app_secret=hs_app_secret, sleep_on_rate_limit_exceeded=True)
    return hs, engine

def update_mailboxes(engine, hs_app_id, hs_app_secret, dis, die):
    hs, engine = create_HS_engine(engine, hs_app_id, hs_app_secret)
    #Importem totes les mailboxes de Som Energia a HelpScout
    mailboxes = hs.mailboxes.get()

    #per tema idempotencia nomes les mes noves de l'ultima execucio
    mailboxes_insert = [HS_mailbox(id=m.id, email=m.email, name=m.name) for m in mailboxes if dis < pendulum.parse(m.createdAt) and pendulum.parse(m.createdAt) <= die]
    print(f"insertem {len(mailboxes_insert)} mailboxes")
    with Session(engine) as session:
        with session.begin():
            session.add_all(mailboxes_insert)

if __name__ == '__main__':
    args = sys.argv[1:]
    dis = pendulum.parse(args[0])
    die = pendulum.parse(args[1])
    engine = create_engine(args[2])
    hs_app_id = args[3]
    hs_app_secret = args[4]

    update_mailboxes(engine, hs_app_id, hs_app_secret, dis, die)





