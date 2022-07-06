import sys
from pathlib import Path
from subprocess import Popen, PIPE
from typing import Iterator, NamedTuple
from time import sleep

from pytest import fixture

from clickhouse_connect.driver import default_port
from clickhouse_connect.driver.client import Client
from clickhouse_connect import create_client
from clickhouse_connect.driver.exceptions import ClickHouseError


class TestConfig(NamedTuple):
    interface: str
    host: str
    port: int
    username: str
    password: str
    use_docker: bool
    test_database: str
    cleanup: bool
    local: bool

    @property
    def cloud(self):
        return self.host.endswith('clickhouse.cloud')


@fixture(scope='session', autouse=True, name='test_config')
def test_config_fixture(request) -> Iterator[TestConfig]:
    interface = request.config.getoption('interface')
    host = request.config.getoption('host')
    port = request.config.getoption('port')
    if not interface:
        interface = 'http' if host == 'localhost' else 'https'
    use_docker = request.config.getoption('docker', True) and host == 'localhost'
    if not port:
        if use_docker:
            port = 10723
        else:
            port = default_port(interface, secure=interface == 'https')
    username = request.config.getoption('username')
    password = request.config.getoption('password')
    cleanup = request.config.getoption('cleanup')
    local = request.config.getoption('local')
    test_database = request.config.getoption('test_db', None)
    if test_database:
        cleanup = False
    else:
        test_database = 'cc_test'
    yield TestConfig(interface, host, port, username, password, use_docker, test_database, cleanup, local)


@fixture(scope='session', name='test_db')
def test_db_fixture(test_config: TestConfig) -> Iterator[str]:
    yield test_config.test_database or 'default'


@fixture(scope='session', name='test_table_engine')
def test_table_engine_fixture(test_config:TestConfig) -> Iterator[str]:
    yield 'ReplicatedMergeTree' if test_config.cloud else 'MergeTree'


@fixture(scope='session', autouse=True, name='test_client')
def test_client_fixture(test_config: TestConfig, test_db: str) -> Iterator[Client]:
    compose_file = f'{Path(__file__).parent}/docker-compose.yml'
    if test_config.use_docker:
        run_cmd(['docker-compose', '-f', compose_file, 'down', '-v'])
        sys.stderr.write('Starting docker compose')
        up_result = run_cmd(['docker-compose', '-f', compose_file, 'up', '-d'])
        if up_result[0]:
            raise Exception(f'Failed to start docker: {up_result[2]}')
        sleep(5)
    tries = 0
    while True:
        tries += 1
        try:
            client = create_client(interface=test_config.interface,
                                   host=test_config.host,
                                   port=test_config.port,
                                   username=test_config.username,
                                   password=test_config.password,
                                   allow_suspicious_low_cardinality_types=True)
            break
        except ClickHouseError as ex:
            if tries > 15:
                raise Exception('Failed to connect to ClickHouse server after 30 seconds') from ex
            sleep(1)
    if test_db != 'default':
        client.command(f'CREATE DATABASE IF NOT EXISTS {test_db}', use_database=False)
        client.database = test_db
    yield client
    if test_config.use_docker:
        down_result = run_cmd(['docker-compose', '-f', compose_file, 'down', '-v'])
        if down_result[0]:
            sys.stderr.write(f'Warning -- failed to cleanly bring down docker compose: {down_result[2]}')
        else:
            sys.stderr.write('Successfully stopped docker compose')


def run_cmd(cmd):
    with Popen(cmd, stdout=PIPE, stderr=PIPE) as popen:
        stdout, stderr = popen.communicate()
        return popen.returncode, stdout, stderr
