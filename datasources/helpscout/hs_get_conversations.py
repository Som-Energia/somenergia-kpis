from helpscout.client import HelpScout
import pandas as pd
from sqlalchemy import create_engine
import datetime
from datetime import date
from datetime import timedelta

def create_HS_engine(engine, hs_app_id, hs_app_secret):
    hs = HelpScout(app_id=hs_app_id, app_secret=hs_app_secret, sleep_on_rate_limit_exceeded=True)
    return hs, engine

def set_params(inici=None, fi=None):
    """
    Auto:
        Només correus tancats i de l'última setmana. Pensat per ser executat cada dilluns.
    Manual:
        El mateix però triem l'interval de dades
    """
    if None in (inici, fi):
        lastSunday = date.today() - timedelta(days=(date.today().weekday() - 6) % 7)
        lastMonday = lastSunday - timedelta(days=(lastSunday.weekday() - 0) % 7)
        lastSunday = lastSunday.strftime("%Y-%m-%dT23:59:59Z")
        lastMonday = lastMonday.strftime("%Y-%m-%dT00:00:00Z")
        params = 'query=(modifiedAt:['+lastMonday+' TO '+lastSunday+'])&status=closed' 
        return params
    else:
        dataInici = inici + "T00:00:00Z"
        dataFi = fi + "T23:59:59Z"
        params = 'query=(modifiedAt:['+dataInici+' TO '+dataFi+'])&status=closed' 
        return params


def get_conversations(params, hs, engine):
    """
    només som capaços de filtrar dates amb modifiedAt i createdAt. Ni close, ni res més sembla que tingui paràmetre propi a 
    la API. Data modificat és = a data de close pq és l'última acció, suposem:
    """
    conversations = hs.conversations.get(params=params)
    import pdb; pdb.set_trace()
    convtags = pd.DataFrame(
        [[
            c.id,
            c.createdAt,
            c.closedAt,
            c.mailboxId,
            [x['tag'] for x in c.tags],
            c.status
        ] for c in conversations],
        columns=['id_conv','hs_created_at','hs_closed_at','mailbox_id','tag','status']
    )

    convtags = convtags.explode('tag')

    convtags['hs_created_at'] = pd.to_datetime(convtags['hs_created_at'])
    convtags['hs_closed_at'] = pd.to_datetime(convtags['hs_closed_at'])
    convtags['inserted_at'] = datetime.datetime.now()
    convtags.to_sql('hs_gcontractes_tags_test', engine, index = False, if_exists = 'append')


def update_hs_conversations(engine, hs_app_id, hs_app_secret, verbose=2, dry_run=False, inici=None, fi=None):
    # TODO: Actual mian function does not support custom params for each pipe/update function (example: inici, fi)
    hs, engine = create_HS_engine(engine, hs_app_id, hs_app_secret)
    inici = '2022-03-14'
    fi = '2022-03-14'
    params = set_params(inici,fi)
    if verbose > 1:
        print(f"Let's get conversations with params: {params}")
    get_conversations(params, hs, engine)



#cs = [c.__dict__ for c in conversations]
#pd.json_normalize(cs)


Conversation(_embedded={'threads': []}, _links={'self': {'href': 'https://api.helpscout.net/v2/conversations/1815740677'}, 'mailbox': {'href': 'https://api.helpscout.net/v2/mailboxes/25135'}, 'primaryCustomer': {'href': 'https://api.helpscout.net/v2/customers/12079468'}, 'createdByCustomer': {'href': 'https://api.helpscout.net/v2/customers/12079468'}, 'closedBy': {'href': 'https://api.helpscout.net/v2/users/1'}, 'threads': {'href': 'https://api.helpscout.net/v2/conversations/1815740677/threads/'}, 'web': {'href': 'https://secure.helpscout.net/conversation/1815740677/11422849'}}, bcc=[], cc=['jsolanamu@gmail.com'], closedAt="2022-03-14T23:20:15Z", closedBy=1, closedByUser={'id': 1, 'type': 'user', 'first': 'Help', 'last': 'Scout', 'email': 'none@nowhere.com'}, createdAt="2022-03-14T23:20:15Z", createdBy={'id': 12079468, 'type': 'customer', 'first': 'Modificacions', 'last': 'Contractuals', 'photoUrl': 'https://d33v4339jhl8k0.cloudfront.net/customer-avatar/04.png', 'email': 'modifica@somenergia.coop'}, customFields=[], customerWaitingSince={'time': '2022-03-14T23:20:15Z', 'friendly': 'Mar 15'}, folderId=255025, id=1815740677, mailboxId=25135, number=11422849, preview="Hola,  Hemos recibido una solicitud de modificación del contrato 0074518 con Som Energia:  Datos del contrato:  - CUPS: ES0031300000491024GY0F  - Dirección: Pasaje Canal de Berdún, 1, 10°2a 22004 ", primaryCustomer={'id': 12079468, 'type': 'customer', 'first': 'Modificacions', 'last': 'Contractuals', 'photoUrl': 'https://d33v4339jhl8k0.cloudfront.net/customer-avatar/04.png', 'email': 'modifica@somenergia.coop'}, source={'type': 'email', 'via': 'customer'}, state="published", status="closed", subject="Som Energia: Sol·licitud de modificació de contracte 0074518. Solicitud de modificación de contrato 0074518.", tags=[], threads=1, type="email", userUpdatedAt="2022-03-14T23:20:15Z")
(Pdb) type(conversations[0])
<class 'helpscout.model.Conversation'>