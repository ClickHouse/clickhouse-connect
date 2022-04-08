def pytest_addoption(parser):
    parser.addoption('--docker', default=True, action='store_true')
    parser.addoption('--no-docker', dest='docker', action='store_false')
    parser.addoption('--host',  help='ClickHouse host', default='127.0.0.1')
    parser.addoption('--port', type=int, help='ClickHouse http port')
    parser.addoption('--cleanup', default=True, action='store_true')
    parser.addoption('--no-cleanup', dest='cleanup', action='store_false')
