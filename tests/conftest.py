import os
import time
import pytest

from clickhouse_connect.datatypes.format import clear_all_formats

os.environ['TZ'] = 'UTC'
time.tzset()


@pytest.fixture(autouse=True)
def clean_global_state():
    clear_all_formats()
