import typer

app = typer.Typer()

@app.command()
def say_hello(dbapi: str):
    print('hello world')
    print(dbapi)

if __name__ == '__main__':
  app()