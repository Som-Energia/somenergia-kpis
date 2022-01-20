import unittest

import pandas as pd
import numpy as np
import datetime
from pathlib import Path
import pytz

from common.utils import dateCETstr_to_CETtzdt

from pipelines.energy_budget import (
    joined_timeseries,
    hourly_energy_budget,
    interpolated_last_meff_prices_by_hour,
    pipe_hourly_energy_budget,
)

from sqlalchemy import create_engine, Table, MetaData

import dbconfig