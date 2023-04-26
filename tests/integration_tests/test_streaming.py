import random
import string

from clickhouse_connect.driver import Client
from clickhouse_connect.driver.exceptions import StreamClosedError, ProgrammingError, StreamFailureError


def test_row_stream(test_client: Client):
    row_stream = test_client.query_rows_stream('SELECT number FROM numbers(10000)')
    total = 0
    with row_stream:
        for row in row_stream:
            total += row[0]
    try:
        with row_stream:
            pass
    except StreamClosedError:
        pass
    assert total == 49995000


def test_column_block_stream(test_client: Client):
    random_string = 'randomStringUTF8(50)'
    if not test_client.min_version('20'):
        random_string = random.choices(string.ascii_lowercase, k=50)
    block_stream = test_client.query_column_block_stream(f'SELECT number, {random_string} FROM numbers(10000)',
                                                         settings={'max_block_size': 4000})
    total = 0
    block_count = 0
    with block_stream:
        for block in block_stream:
            block_count += 1
            total += sum(block[0])
    assert total == 49995000
    assert block_count > 1


def test_row_block_stream(test_client: Client):
    random_string = 'randomStringUTF8(50)'
    if not test_client.min_version('20'):
        random_string = random.choices(string.ascii_lowercase, k=50)
    block_stream = test_client.query_row_block_stream(f'SELECT number, {random_string} FROM numbers(10000)',
                                                      settings={'max_block_size': 4000})
    total = 0
    block_count = 0
    with block_stream:
        for block in block_stream:
            block_count += 1
            for row in block:
                total += row[0]
    assert total == 49995000
    assert block_count > 1


def test_stream_errors(test_client: Client):
    query_result = test_client.query('SELECT number FROM numbers(100000)')
    try:
        for _ in query_result.row_block_stream:
            pass
    except ProgrammingError as ex:
        assert 'context' in str(ex)
    assert query_result.row_count == 100000
    try:
        with query_result.rows_stream as stream:
            assert sum(row[0] for row in stream) == 3882
    except StreamClosedError:
        pass


def test_stream_failure(test_client: Client):
    with test_client.query_row_block_stream('SELECT toString(cityHash64(number)) FROM numbers(10000000)' +
                                            ' where intDiv(1,number-300000)>-100000000') as stream:
        blocks = 0
        failed = False
        try:
            for _ in stream:
                blocks += 1
        except StreamFailureError as ex:
            failed = True
            assert 'division by zero' in str(ex).lower()
    assert failed
