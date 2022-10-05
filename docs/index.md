# Welcome to SomEnergia KPIs Documentation MKdocs

The github's [Readme.md](https://github.com/Som-Energia/somenergia-kpis) is a good place to start.
But if you're a dev making a new kpi, this will get you up to speed in 'I know kung-fu' style.

# KPI wars: A new kpi

We have a script (`filtered_models_single_kpis.py`) that connects to the erp via erppeek
and queries the kpis stated in a table called `erppeek_kpis_description`. So we first need to create that table.

```bash
$ python scripts/csv_to_sqltable.py --csvpath "datasources/erppeek/erppeek_kpis_test.csv" --dbapi "postgresql://somenergia:PASSWORD@puppis.somenergia.lan:5432/sandbox" --schema dbt_vader --table erppeek_kpis_description --ifexists replace
```

Now the kpis listed in `erppeek_kpis_test.csv` will be in a table under the schema `dbt_vader`.
We can now run the `fitlered_models_single_kpis` script to compute them querying the ERP via erppeek.

```bash
$ python datasources/erppeek/filtered_models_single_kpis.py postgresql://somenergia:PASSWORD@puppis.somenergia.lan:5432/sandbox daily ERP_URL somenergia ERP_USER ERP_PASSWORD dbt_vader
```

Now we should have three tables with values, the EL is done and we are ready to Transform :rocket:

- name: erppeek_kpis_description
- name: pilotatge_int_kpis
- name: pilotatge_float_kpis

```bash
$ dbt run --target testing --project-dir ./dbt_kpis -m kpis_raw+
```

You now have the `utils/show_query.sql` which you can set to whichever query you'd want dbt to output in the command-line
if you don't want to go and check the tables yourself :smile:

# About mkdocs

For full documentation on mkdocs visit [mkdocs.org](https://www.mkdocs.org).

In particular we are using [mkdocs-material](https://squidfunk.github.io/mkdocs-material/getting-started/)

However, a quickstart is

## Commands

* `pip install mkdocs-material`
* (`mkdocs new [dir-name]` - Create a new project.) not necessary
* `mkdocs serve` - Start the live-reloading docs server.
* `mkdocs build` - Build the documentation site.
* `mkdocs -h` - Print help message and exit.

## Project layout

    mkdocs.yml    # The configuration file.
    docs/
        index.md  # The documentation homepage.
        ...       # Other markdown pages, images and other files.