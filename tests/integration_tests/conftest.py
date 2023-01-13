import sys
import os
import random
import time
from pathlib import Path
from subprocess import Popen, PIPE
from typing import Iterator, NamedTuple, Sequence, Optional

from pytest import fixture

from clickhouse_connect.driver.client import Client
from clickhouse_connect import create_client
from clickhouse_connect.driver.exceptions import OperationalError
from tests.helpers import TableContext


class TestConfig(NamedTuple):
    host: str
    port: int
    username: str
    password: str
    docker: bool
    test_database: str
    cloud: bool
    __test__ = False


@fixture(scope='session', autouse=True, name='test_config')
def test_config_fixture() -> Iterator[TestConfig]:
    host = os.environ.get('CLICKHOUSE_CONNECT_TEST_HOST', 'localhost')
    docker = host == 'localhost' and \
        os.environ.get('CLICKHOUSE_CONNECT_TEST_DOCKER', 'True').lower() in ('true', '1', 'y', 'yes')
    port = int(os.environ.get('CLICKHOUSE_CONNECT_TEST_PORT', '0'))
    if not port:
        port = 10723 if docker else 8123
    cloud = os.environ.get('CLICKHOUSE_CONNECT_TEST_CLOUD', 'True').lower() in ('true', '1', 'y', 'yes')
    username = os.environ.get('CLICKHOUSE_CONNECT_TEST_USER', 'default')
    password = os.environ.get('CLICKHOUSE_CONNECT_TEST_PASSWORD', '')
    test_database = f'ch_connect__{random.randint(100000, 999999)}__{int(time.time() * 1000)}'
    yield TestConfig(host, port, username, password, docker, test_database, cloud)


@fixture(scope='session', name='test_db')
def test_db_fixture(test_config: TestConfig) -> Iterator[str]:
    yield test_config.test_database or 'default'


@fixture(scope='session', name='test_table_engine')
def test_table_engine_fixture() -> Iterator[str]:
    yield 'MergeTree'


@fixture(scope='session', autouse=True, name='test_client')
def test_client_fixture(test_config: TestConfig, test_db: str) -> Iterator[Client]:
    compose_file = f'{Path(__file__).parent}/docker-compose.yml'
    if test_config.docker:
        run_cmd(['docker-compose', '-f', compose_file, 'down', '-v'])
        sys.stderr.write('Starting docker compose')
        pull_result = run_cmd(['docker-compose', '-f', compose_file, 'pull'])
        if pull_result[0]:
            raise Exception(f'Failed to pull latest docker image(s): {pull_result[2]}')
        up_result = run_cmd(['docker-compose', '-f', compose_file, 'up', '-d'])
        if up_result[0]:
            raise Exception(f'Failed to start docker: {up_result[2]}')
        time.sleep(5)
    tries = 0
    while True:
        tries += 1
        try:
            client = create_client(host=test_config.host,
                                   port=test_config.port,
                                   username=test_config.username,
                                   password=test_config.password,
                                   send_progress=False,
                                   compression='br',
                                   settings={
                                       'allow_suspicious_low_cardinality_types': True
                                   }
                                   )
            break
        except OperationalError as ex:
            if tries > 10:
                raise Exception('Failed to connect to ClickHouse server after 30 seconds') from ex
            time.sleep(3)
    if client.min_version('22.6.1'):
        client.set_client_setting('allow_experimental_object_type', 1)
    client.command(f'CREATE DATABASE IF NOT EXISTS {test_db}', use_database=False)
    client.database = test_db
    yield client

    client.command(f'DROP database IF EXISTS {test_db}', use_database=False)
    if test_config.docker:
        down_result = run_cmd(['docker-compose', '-f', compose_file, 'down', '-v'])
        if down_result[0]:
            sys.stderr.write(f'Warning -- failed to cleanly bring down docker compose: {down_result[2]}')
        else:
            sys.stderr.write('Successfully stopped docker compose')


@fixture(scope='session', name='table_context')
def table_context_fixture(test_client: Client, test_table_engine: str):
    def context(name: str,
                columns: Sequence[str],
                column_types: Optional[Sequence[str]] = None,
                order_by: Optional[str] = None):
        return TableContext(test_client, name, columns, column_types, test_table_engine, order_by)

    yield context


def run_cmd(cmd):
    with Popen(cmd, stdout=PIPE, stderr=PIPE) as popen:
        stdout, stderr = popen.communicate()
        return popen.returncode, stdout, stderr
