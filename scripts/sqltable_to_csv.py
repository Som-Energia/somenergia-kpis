#!/usr/bin/env python

from common.db_utils import table_to_csv, create_engine
import sys
import dbconfig


engine = create_engine(dbconfig.puppis_prod_db['dbapi'])

table_to_csv(engine, sys.argv[1], sys.stdout)


