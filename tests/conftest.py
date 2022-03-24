def pytest_addoption(parser):
    parser.addoption('--docker', dest='docker', action='store_true')
    parser.addoption('--no-docker', dest='docker', action='store_false')
    parser.addoption('--host',  help="ClickHouse host", default='localhost')
    parser.addoption('--port', type=int, help='ClickHouse http port')