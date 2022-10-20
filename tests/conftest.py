import os
import time
import pytest

from clickhouse_connect.datatypes.format import clear_all_formats

os.environ['TZ'] = 'UTC'
time.tzset()


@pytest.fixture(autouse=True)
def clean_global_state():
    clear_all_formats()


def pytest_addoption(parser):
    parser.addoption('--tls', default=False, action='store_true')
    parser.addoption('--no-tls', dest='tls', action='store_false')
