import click
import pandas as pd

@click.command()
@click.option('--csvpath', help='Path of origin csv')
@click.option('--dbapi', help='DBApi of target DB')
@click.option('--schema', help='Schema of target DB')
@click.option('--table', help='table name of target DB')
@click.option('--ifexists', help='Append or replace')
def csv_to_sqltable(csvpath,dbapi,schema,table,ifexists):
    click.echo(f"Let's insert a CSV")
    df = pd.read_csv(csvpath)
    df.to_sql(table, con=dbapi, schema=schema, if_exists=ifexists, index=False)
    click.echo(f"CSV inserted")

if __name__ == '__main__':
    csv_to_sqltable()