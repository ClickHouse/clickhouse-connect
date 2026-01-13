import random
import string
import pytest

from clickhouse_connect.driver.exceptions import StreamClosedError, ProgrammingError, StreamFailureError


def test_row_stream(param_client, call, consume_stream):
    stream = call(param_client.query_rows_stream, 'SELECT number FROM numbers(10000)')
    total = 0

    def process(row):
        nonlocal total
        total += row[0]

    consume_stream(stream, process)

    # Verify stream is closed by trying to consume it again
    # This logic relies on consume_stream handling the context manager which checks state
    with pytest.raises(StreamClosedError):
        consume_stream(stream, lambda x: None)

    assert total == 49995000


def test_column_block_stream(param_client, call, consume_stream):
    random_string = 'randomStringUTF8(50)'
    if not param_client.min_version('20'):
        random_string = random.choices(string.ascii_lowercase, k=50)
    stream = call(param_client.query_column_block_stream,
                  f'SELECT number, {random_string} FROM numbers(10000)',
                  settings={'max_block_size': 4000})
    total = 0
    block_count = 0

    def process(block):
        nonlocal total, block_count
        block_count += 1
        total += sum(block[0])

    consume_stream(stream, process)

    assert total == 49995000
    assert block_count > 1


def test_row_block_stream(param_client, call, consume_stream):
    random_string = 'randomStringUTF8(50)'
    if not param_client.min_version('20'):
        random_string = random.choices(string.ascii_lowercase, k=50)
    stream = call(param_client.query_row_block_stream,
                  f'SELECT number, {random_string} FROM numbers(10000)',
                  settings={'max_block_size': 4000})
    total = 0
    block_count = 0

    def process(block):
        nonlocal total, block_count
        block_count += 1
        for row in block:
            total += row[0]

    consume_stream(stream, process)

    assert total == 49995000
    assert block_count > 1


def test_stream_errors(param_client, call, client_mode, consume_stream):
    query_result = call(param_client.query, 'SELECT number FROM numbers(100000)')

    # 1. Test accessing without context manager raises error
    if client_mode == 'sync':
        with pytest.raises(ProgrammingError, match="context"):
            for _ in query_result.row_block_stream:
                pass
    else:
        async def try_iter():
            async for _ in query_result.row_block_stream:
                pass
        with pytest.raises((ProgrammingError, TypeError)):
            call(try_iter)

    assert query_result.row_count == 100000

    # 2. Test that previous access consumed the generator, so next access raises StreamClosedError
    with pytest.raises(StreamClosedError):
        # Note: query_result.rows_stream creates a NEW StreamContext, but its internal generator
        # (self._block_gen) was consumed by the property access in step 1.
        consume_stream(query_result.rows_stream)


def test_stream_failure(param_client, call, consume_stream):
    query = ('SELECT toString(cityHash64(number)) FROM numbers(10000000)' +
             ' where intDiv(1,number-300000)>-100000000')

    stream = call(param_client.query_row_block_stream, query)
    blocks = 0
    failed = False

    def process(block):  # pylint: disable=unused-argument
        nonlocal blocks
        blocks += 1

    try:
        consume_stream(stream, process)
    except StreamFailureError as ex:
        failed = True
        assert 'division by zero' in str(ex).lower()

    assert failed


def test_raw_stream(param_client, call, consume_stream):
    """Test raw_stream for streaming response."""
    chunks = []
    stream = call(param_client.raw_stream, "SELECT number FROM system.numbers LIMIT 1000", fmt="TabSeparated")

    def process(chunk):
        nonlocal chunks
        chunks.append(chunk)

    consume_stream(stream, process)

    assert len(chunks) > 0
    full_data = b"".join(chunks)
    assert len(full_data) > 0
