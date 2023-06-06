import threading

from clickhouse_connect.driver import Client
from clickhouse_connect.driver.exceptions import ProgrammingError


def test_threading_error(test_client: Client):
    thrown = None

    class QueryThread (threading.Thread):
        def run(self):
            nonlocal thrown
            try:
                test_client.command('SELECT randomAscii(512) FROM numbers(1000000)')
            except ProgrammingError as ex:
                thrown = ex

    threads = [QueryThread(), QueryThread()]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert 'concurrent' in str(thrown)
