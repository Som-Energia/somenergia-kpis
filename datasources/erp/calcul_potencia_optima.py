import typer
import pandas as pd
import sqlalchemy
from sqlalchemy.sql import text
import logging

POLISSA_TARIFA_20TD = 43
POLISSA_STATUS_ACTIVE = "'activa'"

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
app = typer.Typer()

@app.command()
def connexion(
        dbapi: str = typer.Option(None, help='DBApi of origin DB'),
        dbapi_dades: str = typer.Option(None, help='DBApi of target DB')
    ):
    logging.info(f"Let's read the database!")
    dbapi_engine = sqlalchemy.create_engine(dbapi)

    with dbapi_engine.connect() as dbapi_connection:
        logging.info(f"DB connection succesfully to {dbapi_engine.url.database}")

        sql_stament = text(f""" WITH polissa_filtered AS
                                    (SELECT id, tarifa, name, state, f1_maximeter.last_maximeter_date
                                    FROM public.giscedata_polissa AS polissa
                                    LEFT JOIN
                                        (SELECT polissa_id, MAX(data_final) AS last_maximeter_date
                                        FROM public.giscedata_f1_maximetre_consumidor
                                        GROUP BY polissa_id) AS f1_maximeter ON f1_maximeter.polissa_id = polissa.id
                                    WHERE tarifa = {POLISSA_TARIFA_20TD} and state = {POLISSA_STATUS_ACTIVE})
                                SELECT polissa_filtered.id, polissa_filtered.name, polissa_filtered.tarifa, polissa_filtered.state, maximeter.data_inici, maximeter.data_final, maximeter.periode_id, maximeter.maximetre, current_power_period.potencia
                                FROM polissa_filtered
                                INNER JOIN giscedata_f1_maximetre_consumidor AS maximeter ON polissa_filtered.id = maximeter.polissa_id AND polissa_filtered.last_maximeter_date = maximeter.data_final
                                LEFT JOIN giscedata_polissa_potencia_contractada_periode AS current_power_period ON current_power_period.polissa_id = polissa_filtered.id AND current_power_period.periode_id = maximeter.periode_id;""")
        dbapi_sql_statement_result = dbapi_connection.execute(sql_stament)
        dbapi_result = dbapi_sql_statement_result.fetchall()

    df = pd.DataFrame(dbapi_result)
    logging.info(f"Dataframe created successfully")
    dbapi_engine_dades = sqlalchemy.create_engine(dbapi_dades)
    df.to_sql(name='calcul_potencia_optima', con=dbapi_engine_dades, if_exists='replace', schema='public', index=False)
    logging.info(f"Dataframe writed successfully into {dbapi_engine_dades.url.database}")

if __name__ == '__main__':
    app()
