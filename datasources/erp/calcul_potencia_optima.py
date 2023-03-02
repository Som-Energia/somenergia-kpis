import typer
import pandas as pd

app = typer.Typer()

'postgres://user:pass@puppis.somenergia.lan:5432/bkmdgf'

@app.command()
def mockup_potencia_optima(puppis_dbapi: str):

    contractes = [1,2,3]
    pa = [4,2,3]
    po = [2.9,4,3]

    dfoptimes = pd.DataFrame({'contracte':contractes, 'potencia_contractada':pa, 'potencia_optima':po})
    dfoptimes.to_sql('beedata_calcul_potencia_optima', puppis_dbapi, schema='prod', if_exists='replace', index=False)
    print("It's done!")

    return None

if __name__ == '__main__':
  app()