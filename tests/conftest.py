import os
import time
import pytz
import pytest

from clickhouse_connect.driver.context import BaseQueryContext

from clickhouse_connect.datatypes.format import clear_all_formats

os.environ['TZ'] = 'UTC'
time.tzset()
BaseQueryContext.local_tz = pytz.UTC


@pytest.fixture(autouse=True)
def clean_global_state():
    clear_all_formats()
