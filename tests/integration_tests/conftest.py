import sys
import os
import random
import time
from subprocess import Popen, PIPE
from typing import Iterator, NamedTuple, Sequence, Optional, Callable

from pytest import fixture

from clickhouse_connect import common
from clickhouse_connect.driver.common import coerce_bool
from clickhouse_connect.driver.exceptions import OperationalError
from clickhouse_connect.tools.testing import TableContext
from clickhouse_connect.driver.httpclient import HttpClient
from clickhouse_connect.driver import AsyncClient, Client, create_client
from tests.helpers import PROJECT_ROOT_DIR


class TestConfig(NamedTuple):
    host: str
    port: int
    username: str
    password: str
    docker: bool
    test_database: str
    cloud: bool
    compress: str
    insert_quorum: int
    proxy_address: str
    __test__ = False


class TestException(BaseException):
    pass


@fixture(scope='session', autouse=True, name='test_config')
def test_config_fixture() -> Iterator[TestConfig]:
    common.set_setting('max_connection_age', 15)  # Make sure resetting connections doesn't break stuff
    host = os.environ.get('CLICKHOUSE_CONNECT_TEST_HOST', 'localhost')
    docker = host == 'localhost' and coerce_bool(os.environ.get('CLICKHOUSE_CONNECT_TEST_DOCKER', 'False'))
    port = int(os.environ.get('CLICKHOUSE_CONNECT_TEST_PORT', '0'))
    if not port:
        port = 8123
    cloud = coerce_bool(os.environ.get('CLICKHOUSE_CONNECT_TEST_CLOUD', 'False'))
    username = os.environ.get('CLICKHOUSE_CONNECT_TEST_USER', 'default')
    password = os.environ.get('CLICKHOUSE_CONNECT_TEST_PASSWORD', '')
    test_database = os.environ.get('CLICKHOUSE_CONNECT_TEST_DATABASE',
                                   f'ch_connect__{random.randint(100000, 999999)}__{int(time.time() * 1000)}')
    compress = os.environ.get('CLICKHOUSE_CONNECT_TEST_COMPRESS', 'True')
    insert_quorum = int(os.environ.get('CLICKHOUSE_CONNECT_TEST_INSERT_QUORUM', '0'))
    proxy_address = os.environ.get('CLICKHOUSE_CONNECT_TEST_PROXY_ADDR', '')
    yield TestConfig(host, port, username, password, docker, test_database, cloud, compress,
                     insert_quorum, proxy_address)


@fixture(scope='session', name='test_db')
def test_db_fixture(test_config: TestConfig) -> Iterator[str]:
    yield test_config.test_database or 'default'


@fixture(scope='session', name='test_create_client')
def test_create_client_fixture(test_config: TestConfig) -> Callable:
    def f(**kwargs):
        client = create_client(host=test_config.host,
                               port=test_config.port,
                               user=test_config.username,
                               password=test_config.password,
                               compress=test_config.compress,
                               settings={'allow_suspicious_low_cardinality_types': 1},
                               client_name='int_tests/test',
                               **kwargs)
        if client.min_version('22.8'):
            client.set_client_setting('database_replicated_enforce_synchronous_settings', 1)
        if client.min_version('24.8') and not test_config.cloud:
            client.set_client_setting('allow_experimental_json_type', 1)
            client.set_client_setting('allow_experimental_dynamic_type', 1)
            client.set_client_setting('allow_experimental_variant_type', 1)
        if test_config.insert_quorum:
            client.set_client_setting('insert_quorum', test_config.insert_quorum)
        elif test_config.cloud:
            client.set_client_setting('select_sequential_consistency', 1)
        client.database = test_config.test_database
        return client

    return f


@fixture(scope='session', name='test_table_engine')
def test_table_engine_fixture() -> Iterator[str]:
    yield 'MergeTree'


# pylint: disable=too-many-branches
@fixture(scope='session', autouse=True, name='test_client')
def test_client_fixture(test_config: TestConfig, test_create_client: Callable) -> Iterator[Client]:
    compose_file = f'{PROJECT_ROOT_DIR}/docker-compose.yml'
    if test_config.docker:
        run_cmd(['docker', 'compose', '-f', compose_file, 'down', '-v'])
        sys.stderr.write('Starting docker compose')
        pull_result = run_cmd(['docker', 'compose', '-f', compose_file, 'pull'])
        if pull_result[0]:
            raise TestException(f'Failed to pull latest docker image(s): {pull_result[2]}')
        up_result = run_cmd(['docker', 'compose', '-f', compose_file, 'up', '-d'])
        if up_result[0]:
            raise TestException(f'Failed to start docker: {up_result[2]}')
        time.sleep(5)
    tries = 0
    if test_config.docker:
        HttpClient.params = {'SQL_test_setting': 'value'}
        HttpClient.valid_transport_settings.add('SQL_test')
    while True:
        tries += 1
        try:
            client = test_create_client()
            break
        except OperationalError as ex:
            if tries > 10:
                raise TestException('Failed to connect to ClickHouse server after 30 seconds') from ex
            time.sleep(3)
    client.command(f'CREATE DATABASE IF NOT EXISTS {test_config.test_database}', use_database=False)
    yield client

    # client.command(f'DROP database IF EXISTS {test_db}', use_database=False)
    if test_config.docker:
        down_result = run_cmd(['docker', 'compose', '-f', compose_file, 'down', '-v'])
        if down_result[0]:
            sys.stderr.write(f'Warning -- failed to cleanly bring down docker compose: {down_result[2]}')
        else:
            sys.stderr.write('Successfully stopped docker compose')


@fixture(scope='session', autouse=True, name='test_async_client')
def test_async_client_fixture(test_client: Client) -> Iterator[AsyncClient]:
    yield AsyncClient(client=test_client)


@fixture(scope='session', name='table_context')
def table_context_fixture(test_client: Client, test_table_engine: str):
    def context(table: str,
                columns: Sequence[str],
                column_types: Optional[Sequence[str]] = None,
                order_by: Optional[str] = None,
                **kwargs):
        if 'engine' not in kwargs:
            kwargs['engine'] = test_table_engine
        return TableContext(test_client,
                            table=table,
                            columns=columns,
                            column_types=column_types,
                            order_by=order_by, **kwargs)

    yield context


def run_cmd(cmd):
    with Popen(cmd, stdout=PIPE, stderr=PIPE) as popen:
        stdout, stderr = popen.communicate()
        return popen.returncode, stdout, stderr
