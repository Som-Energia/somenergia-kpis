import pandas as pd
from sqlalchemy import create_engine
import pandera as pa
from pandera.typing import Series, DateTime
from datetime import timedelta
import sys
import pendulum
import logging

class Employees_summary(pa.SchemaModel):

    employee_id: Series[int] = pa.Field(nullable=False)
    active: Series[bool] = pa.Field(nullable=False)
    gender: Series[str] = pa.Field(nullable=False)
    marital: Series[str] = pa.Field(nullable=False)
    children: Series[int] = pa.Field(nullable=True)
    birthday: Series[DateTime] = pa.Field(nullable=True)
    km_home_work: Series[int] = pa.Field(nullable=True)
    department: Series[str] = pa.Field(nullable=True)
    create_date: Series[DateTime] = pa.Field(nullable=False)
    write_date: Series[DateTime] = pa.Field(nullable=False)
    theoretical_hours_start_date: Series[DateTime] = pa.Field(nullable=True)
    execute_datetime: Series[pd.DatetimeTZDtype] = pa.Field(nullable=False, dtype_kwargs={"unit": "ns", "tz": "UTC"})

    @pa.check("km_home_work")
    def km_home_work_not_over_10k(cls, series: Series[int]) -> Series[bool]:
        """Check that home is not too far away"""
        return series < 10000

def get_raw_hr_employees(engine):
    query = '''
        SELECT
            he.id as employee_id,
            he.active,
            he.gender,
            he.marital,
            he.children,
            he.birthday,
            he.km_home_work,
            hd.name as department,
            he.create_date,
            he.write_date,
            he.theoretical_hours_start_date
        FROM hr_employee AS he
        LEFT JOIN hr_department AS hd
        ON he.department_id = hd.id
        WHERE he.work_email IS NOT NULL
	    AND he.user_id IS NOT NULL
    '''

    return pd.read_sql_query(
        sql=query,
        con=engine,
        coerce_float=False,
        dtype={'children': 'Int64', 'km_home_work': 'Int64'},
        parse_dates=['birthday', 'create_date', 'write_date', 'theoretical_hours_start_date']
    )


def update_odoo_hr_employees(dbapi_source, dbapi_target, execute_datetime):
    engine_odoo = create_engine(dbapi_source).connect()
    engine_puppis = create_engine(dbapi_target).connect()
    df = get_raw_hr_employees(engine_odoo)
    df['execute_datetime'] = pd.to_datetime(execute_datetime, utc=False)
    df_validated = Employees_summary.validate(df)

    logger = logging.getLogger("airflow.task")
    logger.info(f"Inserting {len(df_validated)} records on {execute_datetime}")

    df_validated.to_sql('odoo_hr_employee',con=engine_puppis, if_exists='append', index=False)


if __name__ == '__main__':
    logger = logging.getLogger("airflow.task")
    logger.info(f"Running {' '.join(sys.argv)}")

    args = sys.argv[1:]
    dbapi_source = args[0]
    #dbapi_source = 'postgresql://somarmota:password_url_encoded@10.1.1.199:5432/odoo'
    dbapi_target = args[1]
    execute_datetime=pd.to_datetime(args[2], utc=True)
    update_odoo_hr_employees(dbapi_source, dbapi_target, execute_datetime)