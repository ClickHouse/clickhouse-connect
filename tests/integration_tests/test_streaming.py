import threading

import pytest

from clickhouse_connect import common
from clickhouse_connect.driver.exceptions import ProgrammingError, StreamClosedError, StreamFailureError
from clickhouse_connect.driver.httputil import ResponseSource
from clickhouse_connect.driver.streaming import StreamingResponseSource


@pytest.fixture
def response_reads(monkeypatch, client_mode):
    reads = []
    async_readers = {}
    source_cls = ResponseSource if client_mode == "sync" else StreamingResponseSource
    original_init = source_cls.__init__
    if client_mode == "async":
        reader_cls = pytest.importorskip("aiohttp").StreamReader
        original_read = reader_cls.read

        async def read(reader, n=-1):
            if reader not in async_readers:
                return await original_read(reader, n)
            pending, exhausted = async_readers[reader]
            pending.set()
            try:
                chunk = await original_read(reader, n)
            finally:
                pending.clear()
            if not chunk:
                exhausted.set()
            return chunk

        monkeypatch.setattr(reader_cls, "read", read)

    def initialize(source, response, *args, **kwargs):
        pending = threading.Event()
        exhausted = threading.Event()
        reads.append((pending, exhausted, response))
        if client_mode == "sync":
            original_stream = response.stream

            def stream(*stream_args, **stream_kwargs):
                iterator = original_stream(*stream_args, **stream_kwargs)
                while True:
                    pending.set()
                    try:
                        chunk = next(iterator, None)
                    finally:
                        pending.clear()
                    if chunk is None:
                        exhausted.set()
                        return
                    yield chunk

            monkeypatch.setattr(response, "stream", stream)
        else:
            async_readers[response.content] = pending, exhausted
        original_init(source, response, *args, **kwargs)

    monkeypatch.setattr(source_cls, "__init__", initialize)
    monkeypatch.setattr(common._common_settings["http_buffer_size"], "value", 64 * 1024)
    return reads


@pytest.mark.parametrize("native_codec", ["python", "rust_strict"])
@pytest.mark.parametrize("compress", [False, "lz4"])
def test_early_stream_close_reuses_client(client_factory, call, consume_stream, client_mode, response_reads, native_codec, compress):
    if native_codec == "rust_strict":
        pytest.importorskip("_ch_core")
    # Smoke coverage for early close and reuse across codecs. The slow-read test below is the race regression.
    # Sync clients drain their named session. Async clients cancel and use no session by default.
    client = client_factory(native_codec=native_codec, compress=compress)

    class StopAfterBlockError(Exception):
        pass

    def stop(block):
        assert block[0] == (0,)
        _, exhausted, response = response_reads[-1]
        assert not exhausted.is_set(), "The response was already read to EOF"
        if client_mode == "sync":
            assert not response._original_response.isclosed(), "The HTTP body was already read to EOF"
        else:
            assert not response.content.is_eof(), "The HTTP body was already received in full"
        raise StopAfterBlockError

    for _ in range(2):
        stream = call(
            client.query_row_block_stream,
            "SELECT number FROM numbers(1000000)",
            settings={"max_block_size": 1000, "max_threads": 1, "buffer_size": 1},
        )
        with pytest.raises(StopAfterBlockError):
            consume_stream(stream, stop)
        assert call(client.command, "SELECT 13") == 13


@pytest.mark.parametrize("native_codec", ["python", "rust_strict"])
def test_close_during_slow_stream_read(client_factory, call, consume_stream, client_mode, response_reads, native_codec):
    if native_codec == "rust_strict":
        pytest.importorskip("_ch_core")
    client = client_factory(native_codec=native_codec, compress=False)
    stream = call(
        client.query_row_block_stream,
        "SELECT number, repeat('x', 4096), sleepEachRow(0.0015) FROM numbers(4000)",
        settings={"max_block_size": 1000, "max_threads": 1, "buffer_size": 1},
    )

    class StopAfterBlockError(Exception):
        pass

    def stop(block):
        assert block[0][0] == 0
        pending, exhausted, response = response_reads[-1]
        assert not exhausted.is_set(), "The response was already read to EOF"
        if client_mode == "sync":
            assert not response._original_response.isclosed()
        else:
            assert not response.content.is_eof()
        if client_mode == "sync" and native_codec == "rust_strict":
            assert pending.wait(1), "The read-ahead producer never entered its next transport read"
        elif client_mode == "async":
            assert pending.is_set(), "The async producer has no pending transport read"
        raise StopAfterBlockError

    with pytest.raises(StopAfterBlockError):
        consume_stream(stream, stop)
    assert call(client.command, "SELECT 13") == 13


def test_row_stream(param_client, call, consume_stream):
    stream = call(param_client.query_rows_stream, "SELECT number FROM numbers(10000)")
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
    random_string = "randomStringUTF8(50)"
    stream = call(
        param_client.query_column_block_stream,
        f"SELECT number, {random_string} FROM numbers(10000)",
        settings={"max_block_size": 4000},
    )
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
    random_string = "randomStringUTF8(50)"
    stream = call(
        param_client.query_row_block_stream,
        f"SELECT number, {random_string} FROM numbers(10000)",
        settings={"max_block_size": 4000},
    )
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
    query_result = test_client.query("SELECT number FROM numbers(100000)")

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
    stream = await test_native_async_client.query_row_block_stream("SELECT number FROM numbers(100)")
    async with stream:
        async for _ in stream:
            pass

    # Try to reuse
    with pytest.raises(StreamClosedError):
        async with stream:
            async for _ in stream:
                pass


def test_stream_failure_sync(test_client):
    query = "SELECT toString(cityHash64(number)) FROM numbers(10000000)" + " where intDiv(1,number-300000)>-100000000"

    stream = test_client.query_row_block_stream(query)

    with pytest.raises(StreamFailureError) as excinfo:
        with stream:
            for _ in stream:
                pass

    error_msg = str(excinfo.value).lower()
    # Race condition: may get actual ClickHouse error or generic connection closed
    assert "division by zero" in error_msg or "connection closed" in error_msg


@pytest.mark.asyncio
async def test_stream_failure_async(test_native_async_client):
    query = "SELECT toString(cityHash64(number)) FROM numbers(10000000)" + " where intDiv(1,number-300000)>-100000000"

    stream = await test_native_async_client.query_row_block_stream(query)

    with pytest.raises(StreamFailureError):
        async with stream:
            async for _ in stream:
                pass


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
