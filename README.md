# somenergia-indicadors-KPIs
Repositori per a l'obtenció i manteniment de taules necessàries per a indicadors i KPIs.

# install

```
mkvirtualenv dades
pip install -r requirements.txt
cp dbconfig.example.py dbconfig.py
```

Edit `dbconfig.py` with your data. You will have to create a database.

The visualization user has to have access to new tables by default, because some scripts replace the table each day. You can set this with:

```
ALTER DEFAULT PRIVILEGES IN SCHEMA public
GRANT SELECT ON TABLES TO username;
```

# run

`python main.py --help` or `python main.py --list-functions`

# test

testing will require installing `b2btest` which in turn requires `lxml` to be installed manually via pip