# Rationale darrera de l'ús de DBT

* Status:accepted
* Deciders: Roger, Lucía, Pol
* Date: 2022-09-02

## Context and Problem Statement

Actualment tenim processos de transformació duplicats, sense control de versions ni cap manteniment escampats entre queries del Superset i Redash.

* materialized views incremental per grans volumns de dades
* quan canvio una view costa saber quines views haig de canviar i el deploy és difícil
* És pesat duplicar processos manualent que són pràcticament iguals (agg diari, setmanal, p.e.)

## Decision Drivers

* Materializar incrementalment
* control sobre modificar una taula/view intermitja
* deploy ràpid
* SQL Gitejat
* Automàticament diferents opcions d'una query semblant (agg setmanal, diari, etc)

### Tensions/friccions a explorar

* Por a overdesign: moltes carpetes, molts fitxers, moltes views.. total per fer una suma
* Chaos de nomenclatures de fitxers
* no tenir "funcions" reutilitzables --> macros?
* parametritzacions
* Unit testing o modelatge
* timescale + dbt ? Ningú ho ha fet, perquè?

## Considered Options

* continuous agregates de timescale
* custom scripts (publish.sh ...)
* dbt
* python scripting controlat via Airflow

## Decision Outcome

**timescale**

countinuous agregats de timescale té moltes limitacions

**dbt**

DBT va agafant avantatge.

### Positive Consequences

* Single Truth Simplificat
* No córrer en RAM
* dbt afegeix algunes columnes quality of life : create_date

### Negative Consequences

* No Pandas :( (fal potser serveix com alternativa)
* Fer transformacions complexes en SQL és perjudicial per la salut


## Links

* [dbt](https://docs.getdbt.com)
* [timescale](https://docs.timescale.com/timescaledb/latest/how-to-guides/continuous-aggregates/)
