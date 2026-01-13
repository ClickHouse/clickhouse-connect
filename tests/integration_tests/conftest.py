import asyncio
import sys
import os
import random
import time
from subprocess import Popen, PIPE
from typing import Iterator, NamedTuple, Sequence, Optional, Callable, AsyncContextManager

import pytest_asyncio
from pytest import fixture

from clickhouse_connect import get_async_client
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


# pylint: disable=redefined-outer-name

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
        if client.min_version('24.8') and (client.min_version('24.12') or not test_config.cloud):
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


@fixture(scope="module")
def shared_loop():
    """Shared event loop for running async clients in sync test context."""
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


@fixture(params=["sync", "async"])
def client_mode(request):
    return request.param


@fixture
def call(client_mode, shared_loop):
    """Wrapper to call functions in appropriate sync/async context."""
    if client_mode == "sync":
        return lambda fn, *args, **kwargs: fn(*args, **kwargs)
    return lambda fn, *args, **kwargs: shared_loop.run_until_complete(fn(*args, **kwargs))


@fixture
def consume_stream(client_mode, call):
    """Fixture to consume a stream in either sync or async mode."""

    def _consume(stream, callback=None):
        if client_mode == "sync":
            with stream:
                for item in stream:
                    if callback:
                        callback(item)
        else:

            async def runner():
                async with stream:
                    async for item in stream:
                        if callback:
                            callback(item)

            call(runner)

    return _consume


@fixture
def client_factory(client_mode, test_config, shared_loop):
    """Factory for creating clients with custom configuration in tests."""
    clients = []

    def factory(**kwargs):
        config = {
            "host": test_config.host,
            "port": test_config.port,
            "username": test_config.username,
            "password": test_config.password,
            "database": test_config.test_database,
            "compress": test_config.compress,
            **kwargs,
        }

        if client_mode == "sync":
            client = create_client(**config)
        else:
            client = shared_loop.run_until_complete(get_async_client(**config))

        clients.append(client)
        return client

    yield factory

    for client in clients:
        try:
            if client_mode == "sync":
                client.close()
            else:
                shared_loop.run_until_complete(client.close())
        except Exception:  # pylint: disable=broad-exception-caught
            pass


@fixture
def param_client(client_mode, test_config, shared_loop):
    """Provides client based on client_mode parameter."""
    if client_mode == "sync":
        client = create_client(
            host=test_config.host,
            port=test_config.port,
            username=test_config.username,
            password=test_config.password,
            database=test_config.test_database,
            compress=test_config.compress,
            settings={"allow_suspicious_low_cardinality_types": 1},
            client_name="int_tests/param_sync",
        )
        if client.min_version("22.8"):
            client.set_client_setting("database_replicated_enforce_synchronous_settings", 1)
        if client.min_version("24.8") and (client.min_version("24.12") or not test_config.cloud):
            client.set_client_setting("allow_experimental_json_type", 1)
            client.set_client_setting("allow_experimental_dynamic_type", 1)
            client.set_client_setting("allow_experimental_variant_type", 1)
        if test_config.insert_quorum:
            client.set_client_setting("insert_quorum", test_config.insert_quorum)
        elif test_config.cloud:
            client.set_client_setting("select_sequential_consistency", 1)

        yield client
        client.close()
    else:
        client = shared_loop.run_until_complete(
            get_async_client(
                host=test_config.host,
                port=test_config.port,
                username=test_config.username,
                password=test_config.password,
                database=test_config.test_database,
                compress=test_config.compress,
                settings={"allow_suspicious_low_cardinality_types": 1},
                client_name="int_tests/param_async",
            )
        )

        if client.min_version("22.8"):
            client.set_client_setting("database_replicated_enforce_synchronous_settings", "1")
        if client.min_version("24.8"):
            client.set_client_setting("allow_experimental_json_type", "1")
            client.set_client_setting("allow_experimental_dynamic_type", "1")
            client.set_client_setting("allow_experimental_variant_type", "1")
        if test_config.insert_quorum:
            client.set_client_setting("insert_quorum", str(test_config.insert_quorum))
        elif test_config.cloud:
            client.set_client_setting("select_sequential_consistency", "1")

        yield client
        shared_loop.run_until_complete(client.close())


# pylint: disable=too-many-branches
@fixture(scope='session', autouse=True, name='test_client')
def test_client_fixture(test_config: TestConfig, test_create_client: Callable) -> Iterator[Client]:
    if test_config.docker:
        compose_file = f'{PROJECT_ROOT_DIR}/docker-compose.yml'
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

    # In cloud env, there seems to be some issues with creating a db and then immediately using it.
    # This ensures it's visible before yielding it back to the test.
    visible = False
    for _ in range(30):
        rows = client.query("SELECT name FROM system.databases").result_rows
        if any(r[0] == test_config.test_database for r in rows):
            visible = True
            break
        time.sleep(0.1)
    if not visible:
        raise TestException(f"Database {test_config.test_database} not visible after waiting")
    yield client

    if test_config.docker:
        down_result = run_cmd(['docker', 'compose', '-f', compose_file, 'down', '-v'])
        if down_result[0]:
            sys.stderr.write(f'Warning -- failed to cleanly bring down docker compose: {down_result[2]}')
        else:
            sys.stderr.write('Successfully stopped docker compose')


@pytest_asyncio.fixture(scope='session', name='test_async_client')
async def test_async_client_fixture(test_client: Client) -> AsyncContextManager[AsyncClient]:
    async with AsyncClient(client=test_client) as client:
        yield client


@pytest_asyncio.fixture(scope="function", loop_scope="function", name="test_native_async_client")
async def test_native_async_client_fixture(test_config: TestConfig) -> AsyncContextManager:
    """Function-scoped fixture for aiohttp async client"""
    async with await get_async_client(
        host=test_config.host,
        port=test_config.port,
        username=test_config.username,
        password=test_config.password,
        database=test_config.test_database,
        compress=test_config.compress,
        client_name="int_tests/aiohttp_async",
    ) as client:
        if client.min_version("22.8"):
            client.set_client_setting("database_replicated_enforce_synchronous_settings", "1")
        if client.min_version("24.8"):
            client.set_client_setting("allow_experimental_json_type", "1")
            client.set_client_setting("allow_experimental_dynamic_type", "1")
            client.set_client_setting("allow_experimental_variant_type", "1")
        if test_config.insert_quorum:
            client.set_client_setting("insert_quorum", str(test_config.insert_quorum))
        elif test_config.cloud:
            client.set_client_setting("select_sequential_consistency", "1")

        yield client


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
