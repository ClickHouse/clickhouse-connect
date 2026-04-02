import os
import time

import pytest
import pytz

from clickhouse_connect.datatypes.format import clear_all_formats
from clickhouse_connect.driver import tzutil

os.environ["TZ"] = "UTC"
time.tzset()


@pytest.fixture(autouse=True)
def clean_global_state():
    clear_all_formats()
    tzutil.local_tz = pytz.UTC
