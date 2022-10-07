# How to deploy to production environment

**tl; dr** aka **muxo testo**

```bash

git merge YOUR_BRANCH

python scripts/csv_to_sqltable.py --csvpath "datasources/erppeek/erppeek_kpis_decription.csv" --dbapi "postgresql://somenergia:PASSWORD@puppis.somenergia.lan:5432/dades" --schema prod_operational --table erppeek_kpis_description --ifexists append --truncate

git push

#Run Airflow DAG

dbt run --target prod -m +kpis_row+
```

## Merge your branch into main

:warning: Airflow is in Continuous Delivery, `main` branch will be automatically downloaded in production as soon as a task is run. :warning:

```bash
git pull
git merge main
git checkout main
git merge YOUR_BRANCH
```

When you're ready, push to production

```bash
git push
```

## Update KPIs Description table

From your local machine launch script to update KPIs table from CSV.

**This process overwrites the table.**

Given that dbt views depend on the table, we can't drop it. Therefore we truncate and append.

```bash
python scripts/csv_to_sqltable.py --csvpath "datasources/erppeek/erppeek_kpis_description.csv" --dbapi "postgresql://somenergia:PASSWORD@puppis.somenergia.lan:5432/dades" --schema prod_operational --table erppeek_kpis_description --ifexists append --truncate
```

## Launch Airflow DAG or wait for it

Airflow runs daily tasks reading the kpis table and querying the ERP via erppeek.
You can wait for it to run or run it manually.
DBT already selects the newest run when publishing the kpis to the datamart.

## Run DBT workflow

From your local machine run DBT workflow targeting production environment.

```bash
dbt run --target prod -m +kpis_row+
```
