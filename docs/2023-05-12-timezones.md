# **Timezones segons dades**

# Context

Una mica de la festa de la timezone després de barallar-nos-hi un xic.

mantras: Timestamp is a picture of a clock. You don't want a picture of a clock.

Sobre timestamptz, contrariament al què hom pensaria quan llegeix "timestamp with time zone", aquest datatype de Postgres no guarda un timezone. És un "flag binari" de visualizació, un helper d'inserció i de visualització que converteix de la teva configuració o la de servidor a unix timestamp.

Sobre què fem a dades, coincideix bastant amb el què diu aquí:

https://wiki.postgresql.org/wiki/Don%27t_Do_This#Don.27t_use_timestamp_.28without_time_zone.29

## Glossari

Si posem `[citation needed]` vol dir que encara ho estem analitzant.

`show time zone` és `Europe/Madrid`. Per tant les visualitzacions de timestamptz s'ensenyen en `Europe/Madrid`

# Resumet

Tot datetime a la db en timestamptz i quan insertes has de fer-ho amb el timezone especificat. En general no confiem amb `show time zone;` del servidor o client [citation needed] _perquè potser podríem acceptar agregacions naïf_ .

Agregacions amb timezone `date_trunc('day', some_timestamptz, 'Europe/Madrid')`

Les dates poden ser naïf (no existeix el concepte de date aware a postgres), però seran en local. Si es pot i té sentit, mantenir el timestamptz de mitjanit [citation needed].

# Aprofundim

## types d'entrada

si rebem un timestamp, de seguida el passem a timestamptz amb `at time zone`.

si rebem un date, primer el passem a timestamp

Amb el timezone del client configurat a 'Europe/Madrid', dóna

```sql
select
	'2021-01-01'::date as adate,
	'2021-01-01'::date at time zone 'Europe/Madrid', -- timestamp 😲 ❌
	'2021-01-01'::date::timestamp at time zone 'Europe/Madrid' -- timestamptz 👍
```
|adate|timezone|timezone|
|-----|--------|--------|
|2021-01-01|2021-01-01 00:00:00|2021-01-01 00:00:00+01|

## Sources amb naïf i columna dst

Ho passem a timestamp i convertim a la timezone que correspongui abans d'insertar-ho en timestamptz

```sql
	CASE
		WHEN estiu=1 AND (sistema = 'PEN' or sistema = 'BAL')
			THEN data::timestamp AT TIME ZONE 'CEST'
		WHEN estiu=0 AND (sistema = 'PEN' or sistema = 'BAL')
			THEN data::timestamp AT TIME ZONE 'CET'
		WHEN estiu=1 AND sistema = 'CAN'
		    THEN data::timestamp AT TIME ZONE 'WETDST'
		WHEN estiu=0 AND sistema = 'CAN'
		    THEN data::timestamp AT TIME ZONE 'WET'
	END AS end_hour_aware
```

Idealment afegiríem una columna amb el timezone en sintaxi postgres ('Europe/Madrid', 'Atlantic/Canary') per després poder fer

```sql
select
end_hour_aware,
end_hour_aware at time zone timezone as end_hour_local,
date_trunc('day', end_hour_aware, timezone) as day_local
from (
    values
        ('2021-01-01 10:00:00'::timestamp at time zone 'Europe/Madrid', 'Europe/Madrid'),
        ('2021-01-01 10:00:00'::timestamp at time zone 'Atlantic/Canary', 'Atlantic/Canary')
) as foo(end_hour_aware, timezone);
```

|end_hour_aware|end_hour_local|day_local|
|--------------|--------------|---------|
|2021-01-01 10:00:00+01|2021-01-01 10:00:00|2021-01-01 00:00:00+01|
|2021-01-01 11:00:00+01|2021-01-01 10:00:00|2021-01-01 01:00:00+01|


Podeu veure la casuística:

```sql
select
end_hour_aware at time zone 'UTC' as end_hour_naif_utc_in_db, -- db *always* stores naïf unix timestamps, utc, even if datatype is timestamptz
end_hour_aware as end_hour_aware_at_configured_timezone, -- depends on show time zone; of your client/server
end_hour_aware at time zone timezone as end_hour_local, -- timestamp naïf (can't be otherwise once we localize)
date_trunc('day', end_hour_aware, timezone) as midnight_local, -- midnight local seen by `show time zone;`, it's timestamptz, hence automatically converted for display. really unix_timestamp in db
date_trunc('day', end_hour_aware, timezone)::date as day_local,
date_trunc('day', end_hour_aware)::date as day_naif_local_and_wrong, -- ❌ implicit conversion to `show time zone`
time_bucket('1 day', end_hour_aware, timezone) as day_bucket
from (
    values
        ('2021-01-01 20:00:00'::timestamp at time zone 'Europe/Madrid', 'Europe/Madrid'),
        ('2021-01-01 20:00:00'::timestamp at time zone 'Atlantic/Canary', 'Atlantic/Canary'),
        ('2021-01-01 20:00:00'::timestamp at time zone 'PST', 'PST')
) as foo(end_hour_aware, timezone);
```

|end_hour_naif_utc_in_db|end_hour_aware_at_configured_timezone|end_hour_local|midnight_local|day_local|day_naif_local_and_wrong|day_bucket|
|-----------------------|-------------------------------------|--------------|--------------|---------|------------------------|----------|
|2021-01-01 19:00:00|2021-01-01 20:00:00+01|2021-01-01 20:00:00|2021-01-01 00:00:00+01|2021-01-01|2021-01-01|2021-01-01 00:00:00+01|
|2021-01-01 20:00:00|2021-01-01 21:00:00+01|2021-01-01 20:00:00|2021-01-01 01:00:00+01|2021-01-01|2021-01-01|2021-01-01 01:00:00+01|
|2021-01-02 04:00:00|2021-01-02 05:00:00+01|2021-01-01 20:00:00|2021-01-01 09:00:00+01|2021-01-01|2021-01-02|2021-01-01 09:00:00+01|

## Agregacions

Les agregacions (de calendari) sempre amb el time zone que sigui rellevant. Una agregació diaria està sempre lligada a un timezone concret, perquè el dia està definit només dins d'un timezone, sinó parlaríem d'agregacions de 24h, que faríem en utc.

```sql
select
    -- date_trunc per defecte fa servir el time zone configurat
    date_trunc('day', '2021-01-01 01:00:00+05:00'::timestamptz) as date_trunc_local,
    date_trunc('day', '2021-01-01 01:00:00+05:00'::timestamptz, 'Europe/Madrid') as date_trunc_local_explicit,
    -- time_bucket per defecte fa servir utc
    time_bucket('1 day', '2021-01-01 01:00:00+05:00'::timestamptz) as time_bucket_utc,
    time_bucket('1 day', '2021-01-01 01:00:00+05:00'::timestamptz, 'Europe/Madrid') as time_bucket_local;
```

|date_trunc_local|date_trunc_local_explicit|time_bucket_utc|time_bucket_local|
|----------------|-------------------------|---------------|-----------------|
|2020-12-31 00:00:00+01|2020-12-31 00:00:00+01|2020-12-31 01:00:00+01|2020-12-31 00:00:00+01|


En general ho faríem tot en el time zone de l'Estat, però depèn del use case (Veure [Excepcions a la norma](#excepcions-a-la-norma)).

### Excepcions a la norma

Pel cas que estem tractant actualment, previsió de la demanda, fem servir el dia local (a picture of a clock) perquè el què ens interessa no és comparar intervals de temps, sinó comportaments del dilluns, del cap de setmana, etc. Les hores a agrupar, els dies a agrupar, són culturals, encara que de fet representin packets d'hores universals diferents [citation needed].