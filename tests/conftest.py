import os
import time

os.environ['TZ'] = 'UTC'
time.tzset()


def pytest_addoption(parser):
    parser.addoption('--docker', default=True, action='store_true')
    parser.addoption('--no-docker', dest='docker', action='store_false')
    parser.addoption('--host',  help='ClickHouse host', default='localhost')
    parser.addoption('--port', type=int, help='ClickHouse http port')
    parser.addoption('--interface', help='http or https')
    parser.addoption('--username', help='ClickHouse User', default='default')
    parser.addoption('--password', default = '')
    parser.addoption('--cleanup', default=True, action='store_true')
    parser.addoption('--no-cleanup', dest='cleanup', action='store_false')
    parser.addoption('--test-db', help='Test database, will not be cleaned up')
