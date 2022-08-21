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
    parser.addoption('--docker', default=True, action='store_true')
    parser.addoption('--no-docker', dest='docker', action='store_false')
    parser.addoption('--cloud', default=False, action='store_true')
    parser.addoption('--host',  help='ClickHouse host', default='localhost')
    parser.addoption('--port', type=int, help='ClickHouse http port')
    parser.addoption('--interface', help='http or https')
    parser.addoption('--username', help='ClickHouse User', default='default')
    parser.addoption('--password', default = '')
    parser.addoption('--cleanup', default=True, action='store_true')
    parser.addoption('--no-cleanup', dest='cleanup', action='store_false')
    parser.addoption('--test-db', help='Test database, will not be cleaned up')
    parser.addoption('--tls', default=False, action='store_true')
    parser.addoption('--no-tls', dest='tls', action='store_false')
    parser.addoption('--local', default=False, action='store_true')
