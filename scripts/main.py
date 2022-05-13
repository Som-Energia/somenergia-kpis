import dbconfig
from clean_postal_code import (
    normalize_street_address,
)
from erppeek import Client
from sqlalchemy import create_engine

def task_normalize_street_address():
    client = Client(**dbconfig.erppeek)
    engine = create_engine(dbconfig.test_db['dbapi'])
    normalize_street_address(engine, client)

if __name__ == '__main__':
    results = task_normalize_street_address()