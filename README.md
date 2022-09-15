# somenergia-indicadors-KPIs

## Context

Currently it is necessary to centralize data from different sources to consult with a data visualization application, also to have them in a standardized format.

This tool is used to obtain data from different data sources, store it in a raw format and then load the data for transformation.

There are two modules, the datasources and the pipeline:

- Datasources: extracts through crawlers and saves raw data in a database.

- Pipeline: from the raw data it makes the transformation and performs the most complex operations

![Indicadors_schema](/docs/Indicadors.jpg "Schema")

## install

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

### transformations using dbt

```
$ pip install dbt-postgres
$ dbt init
```
Edit the ~/.dbt/profiles.yml with your connection details. You can use dbt_profile.exemple.yml as an example.
Set the schema to your user as `dbt_<name>`

## run

`python main.py --help` or `python main.py --list-functions`

### dbt

`$ dbt run --target testing --project-dir dbt_kpis`

## test

Testing will require installing `b2btest` which in turn requires `lxml` to be installed manually via pip

Create an empty testing database and configure it in dbconfig.py at the `test_db` entry.

`$ dbt test --target testing --project-dir dbt_kpis`




