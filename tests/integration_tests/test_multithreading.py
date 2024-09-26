import threading

import pytest

from clickhouse_connect.driver import Client
from clickhouse_connect.driver.exceptions import ProgrammingError
from tests.integration_tests.conftest import TestConfig


def test_threading_error(test_config: TestConfig, test_client: Client):
    if test_config.cloud:
        pytest.skip('Skipping threading test in ClickHouse Cloud')
    thrown = None

    class QueryThread (threading.Thread):
        def run(self):
            nonlocal thrown
            try:
                test_client.command('SELECT randomString(512) FROM numbers(1000000)')
            except ProgrammingError as ex:
                thrown = ex

    threads = [QueryThread(), QueryThread()]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert 'concurrent' in str(thrown)
