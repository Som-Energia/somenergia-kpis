import unittest
from unittest.case import skipIf

import pandas as pd
import numpy as np
import datetime
import pendulum

from sqlalchemy import create_engine

import pandera as pa

from datasources.odoo.employees import (
    Employees_summary
)

class OdooEmployeesTest(unittest.TestCase):

    def test__validate_data_frame(self):
        df = pd.DataFrame({
            'employee_id' : [1,2,3,4,5],
            'active' : [True, False, True, True, True],
            'gender' : ['other','male','female','other','other'],
            'marital' : ['married','single','single','single','single'],
            'children' : [0,0,1,0,1],
            'birthday' : ['1990-05-23','1990-05-23','1990-05-23','1990-05-23','1990-05-23'],
            'km_home_work' : [12,67,345,78,4],
            'department' : [None,'IT','IT','IT','IT'],
            'create_date' : ['2022-05-22','2022-05-22','2022-05-22','2022-05-22','2022-05-22'],
            'write_date' : ['2022-05-22','2022-05-22','2022-05-22','2022-05-22','2022-05-22'],
            'theoretical_hours_start_date' : ['2022-05-22','2022-05-22','2022-05-22','2022-05-22','2022-05-22'],
            'execute_datetime' : ['2022-05-22','2022-05-22','2022-05-22','2022-05-22','2022-05-22'],
        })

        df = df.\
            assign(
                birthday=lambda x: pd.to_datetime(x.birthday, utc=False),
                create_date=lambda x: pd.to_datetime(x.create_date, utc=False),
                write_date=lambda x: pd.to_datetime(x.write_date, utc=False),
                theoretical_hours_start_date=lambda x: pd.to_datetime(x.theoretical_hours_start_date, utc=False),
                execute_datetime=lambda x: pd.to_datetime(x.execute_datetime, utc=False)
                )

        Employees_summary.validate(df)

    def test__validate_data_frame__fail(self):
        df = pd.DataFrame({
            'employee_id' : [1,2,3,4,5],
            'active' : [True, False, True, True, True],
            'gender' : ['other','male','female','other','other'],
            'marital' : ['married','single','single','single','single'],
            'children' : [0,0,1,0,1],
            'birthday' : ['1990-05-23','1990-05-23','1990-05-23','1990-05-23','1990-05-23'],
            'km_home_work' : [12,67,345,78,10004],
            'department' : ['laboral','IT','IT','IT','IT'],
            'create_date' : ['2022-05-22','2022-05-22','2022-05-22','2022-05-22','2022-05-22'],
            'write_date' : ['2022-05-22','2022-05-22','2022-05-22','2022-05-22','2022-05-22'],
            'theoretical_hours_start_date' : ['2022-05-22','2022-05-22','2022-05-22','2022-05-22','2022-05-22'],
            'execute_datetime' : ['2022-05-22','2022-05-22','2022-05-22','2022-05-22','2022-05-22'],
        })

        df = df.\
            assign(
                birthday=lambda x: pd.to_datetime(x.birthday, utc=False),
                create_date=lambda x: pd.to_datetime(x.create_date, utc=False),
                write_date=lambda x: pd.to_datetime(x.write_date, utc=False),
                theoretical_hours_start_date=lambda x: pd.to_datetime(x.theoretical_hours_start_date, utc=False),
                execute_datetime=lambda x: pd.to_datetime(x.execute_datetime, utc=False)
                )

        try:
            Employees_summary.validate(df)
        except pa.errors.SchemaError as ex:
            pass