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


def test_stream_errors_sync(test_client):
    query_result = test_client.query('SELECT number FROM numbers(100000)')

    # 1. Test accessing without context manager raises error
    with pytest.raises(ProgrammingError, match="context"):
        for _ in query_result.row_block_stream:
            pass

    assert query_result.row_count == 100000

    # 2. Test that previous access consumed the generator, so next access raises StreamClosedError
    with pytest.raises(StreamClosedError):
        with query_result.rows_stream as stream:
            for _ in stream:
                pass


@pytest.mark.asyncio
async def test_stream_errors_async(test_native_async_client):
    stream = await test_native_async_client.query_row_block_stream('SELECT number FROM numbers(100)')
    async with stream:
        async for _ in stream:
            pass

    # Try to reuse
    with pytest.raises(StreamClosedError):
        async with stream:
            async for _ in stream:
                pass


def test_stream_failure_sync(test_client):
    query = ('SELECT toString(cityHash64(number)) FROM numbers(10000000)' +
             ' where intDiv(1,number-300000)>-100000000')

    stream = test_client.query_row_block_stream(query)
    failed = False

    try:
        with stream:
            for _ in stream:
                pass
    except StreamFailureError as ex:
        failed = True
        assert 'division by zero' in str(ex).lower()

    assert failed


@pytest.mark.asyncio
async def test_stream_failure_async(test_native_async_client):
    query = ('SELECT toString(cityHash64(number)) FROM numbers(10000000)' +
             ' where intDiv(1,number-300000)>-100000000')

    stream = await test_native_async_client.query_row_block_stream(query)
    failed = False

    try:
        async with stream:
            async for _ in stream:
                pass
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
