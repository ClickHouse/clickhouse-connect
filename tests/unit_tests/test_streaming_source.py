import asyncio
import gzip
import threading
import time
import zlib
from unittest.mock import Mock

import lz4.frame
import pytest

from clickhouse_connect.driver.compression import _zstd_compress
from clickhouse_connect.driver.exceptions import GENERIC_CLICKHOUSE_ERROR, OperationalError, StreamFailureError
from clickhouse_connect.driver.streaming import (
    _EXCEPTION_TAIL_SIZE,
    StreamingFileAdapter,
    StreamingInsertSource,
    StreamingResponseSource,
    _ExceptionTail,
)


class MockAsyncIterator:
    """Mock async iterator for simulating aiohttp response content."""

    def __init__(self, chunks):
        self.chunks = chunks
        self.index = 0

    def __aiter__(self):
        return self

    async def __anext__(self):
        if self.index >= len(self.chunks):
            raise StopAsyncIteration
        chunk = self.chunks[self.index]
        self.index += 1
        return chunk


class MockContent:
    """Mock aiohttp StreamReader content."""

    def __init__(self, chunks):
        self.chunks = chunks
        self.index = 0

    async def read(self, n=-1):
        """Mock read method that returns chunks sequentially."""
        if self.index >= len(self.chunks):
            return b""
        chunk = self.chunks[self.index]
        self.index += 1
        return chunk


class MockResponse:
    """Mock aiohttp ClientResponse."""

    def __init__(self, chunks, encoding=None):
        self.content = MockContent(chunks)
        self.headers = {"Content-Encoding": encoding} if encoding else {}
        self.status = 200
        self.closed = False

    def close(self):
        self.closed = True


@pytest.mark.asyncio
async def test_basic_streaming_no_compression():
    """Test basic streaming without compression."""
    chunks = [b"hello ", b"world", b"!"]
    response = MockResponse(chunks)

    source = StreamingResponseSource(response, encoding=None)
    loop = asyncio.get_running_loop()

    await source.start_producer(loop)

    def consume():
        result = []
        for chunk in source.gen:
            result.append(chunk)
        return result

    result = await loop.run_in_executor(None, consume)

    assert result == chunks
    assert b"".join(result) == b"hello world!"


@pytest.mark.asyncio
async def test_streaming_with_gzip_compression():
    """Test streaming with gzip decompression."""
    original_data = b"hello world! " * 1000
    compressed = gzip.compress(original_data)
    chunk_size = 100
    chunks = [compressed[i : i + chunk_size] for i in range(0, len(compressed), chunk_size)]

    response = MockResponse(chunks, encoding="gzip")
    source = StreamingResponseSource(response, encoding="gzip")
    loop = asyncio.get_running_loop()

    await source.start_producer(loop)

    def consume():
        result = []
        for chunk in source.gen:
            result.append(chunk)
        return b"".join(result)

    decompressed = await loop.run_in_executor(None, consume)

    assert decompressed == original_data


@pytest.mark.asyncio
async def test_streaming_with_deflate_compression():
    """Test streaming with deflate decompression."""
    original_data = b"test data " * 500
    compressed = zlib.compress(original_data)

    chunks = [compressed[i : i + 50] for i in range(0, len(compressed), 50)]

    response = MockResponse(chunks, encoding="deflate")
    source = StreamingResponseSource(response, encoding="deflate")
    loop = asyncio.get_running_loop()

    await source.start_producer(loop)

    def consume():
        result = []
        for chunk in source.gen:
            result.append(chunk)
        return b"".join(result)

    decompressed = await loop.run_in_executor(None, consume)

    assert decompressed == original_data


@pytest.mark.asyncio
async def test_streaming_with_zstd_compression():
    """Test streaming with zstd decompression."""
    original_data = b"zstd test data " * 500
    compressed = _zstd_compress(original_data)

    chunks = [compressed[i : i + 50] for i in range(0, len(compressed), 50)]

    response = MockResponse(chunks, encoding="zstd")
    source = StreamingResponseSource(response, encoding="zstd")
    loop = asyncio.get_running_loop()

    await source.start_producer(loop)

    def consume():
        result = []
        for chunk in source.gen:
            result.append(chunk)
        return b"".join(result)

    decompressed = await loop.run_in_executor(None, consume)

    assert decompressed == original_data


@pytest.mark.asyncio
async def test_streaming_with_lz4_compression():
    """Test streaming with lz4 decompression."""
    original_data = b"lz4 test data " * 500
    compressed = lz4.frame.compress(original_data)

    chunks = [compressed[i : i + 50] for i in range(0, len(compressed), 50)]

    response = MockResponse(chunks, encoding="lz4")
    source = StreamingResponseSource(response, encoding="lz4")
    loop = asyncio.get_running_loop()

    await source.start_producer(loop)

    def consume():
        result = []
        for chunk in source.gen:
            result.append(chunk)
        return b"".join(result)

    decompressed = await loop.run_in_executor(None, consume)

    assert decompressed == original_data


@pytest.mark.asyncio
async def test_empty_stream():
    """Test streaming with empty response."""
    response = MockResponse([])
    source = StreamingResponseSource(response, encoding=None)
    loop = asyncio.get_running_loop()

    await source.start_producer(loop)

    def consume():
        result = []
        for chunk in source.gen:
            result.append(chunk)
        return result

    result = await loop.run_in_executor(None, consume)

    assert result == []


@pytest.mark.asyncio
async def test_single_large_chunk():
    """Test streaming with single large chunk."""
    large_chunk = b"x" * 1000000
    response = MockResponse([large_chunk])
    source = StreamingResponseSource(response, encoding=None)
    loop = asyncio.get_running_loop()

    await source.start_producer(loop)

    def consume():
        result = []
        for chunk in source.gen:
            result.append(chunk)
        return result

    result = await loop.run_in_executor(None, consume)

    assert len(result) == 1
    assert result[0] == large_chunk


@pytest.mark.asyncio
async def test_many_small_chunks():
    """Test streaming with many small chunks."""
    chunks = [f"chunk{i}".encode() for i in range(1000)]
    response = MockResponse(chunks)
    source = StreamingResponseSource(response, encoding=None)
    loop = asyncio.get_running_loop()

    await source.start_producer(loop)

    def consume():
        result = []
        for chunk in source.gen:
            result.append(chunk)
        return result

    result = await loop.run_in_executor(None, consume)

    assert len(result) == 1000
    assert result == chunks


@pytest.mark.asyncio
async def test_generator_caching():
    """Test that .gen property returns cached generator."""
    response = MockResponse([b"test"])
    source = StreamingResponseSource(response, encoding=None)
    loop = asyncio.get_running_loop()

    await source.start_producer(loop)

    # Access .gen multiple times - should return same generator
    gen1 = source.gen
    gen2 = source.gen

    assert gen1 is gen2, "Generator should be cached"


@pytest.mark.asyncio
async def test_producer_error_propagation():
    """Test that producer errors are propagated to consumer."""

    class FailingContent:
        @staticmethod
        async def read(n=-1):
            raise ValueError("Producer error!")

    response = Mock()
    response.content = FailingContent()
    response.headers = {}
    response.closed = False

    source = StreamingResponseSource(response, encoding=None)
    loop = asyncio.get_running_loop()

    await source.start_producer(loop)

    def consume():
        try:
            for _ in source.gen:
                pass
        except OperationalError as e:
            return str(e)
        return "No error raised!"

    error_msg = await loop.run_in_executor(None, consume)

    assert error_msg == "Failed to read response data from server"


@pytest.mark.asyncio
async def test_gzip_with_incremental_decompression():
    """Test that gzip decompression works incrementally with streaming."""
    original_data = b"The quick brown fox jumps over the lazy dog. " * 100
    compressed = gzip.compress(original_data)

    # Split compressed data into very small chunks to force incremental decompression
    chunks = [compressed[i : i + 10] for i in range(0, len(compressed), 10)]

    response = MockResponse(chunks, encoding="gzip")
    source = StreamingResponseSource(response, encoding="gzip")
    loop = asyncio.get_running_loop()

    await source.start_producer(loop)

    def consume():
        """Consume and verify we get multiple decompressed chunks."""
        chunks_received = []
        for chunk in source.gen:
            chunks_received.append(chunk)
        return chunks_received, b"".join(chunks_received)

    chunks_received, decompressed = await loop.run_in_executor(None, consume)

    assert decompressed == original_data
    assert len([c for c in chunks_received if c]) > 0


@pytest.mark.asyncio
async def test_backpressure_with_bounded_queue():
    """Test that bounded queue provides backpressure."""
    # Create many chunks to test backpressure
    chunks = [f"chunk{i}".encode() for i in range(100)]
    response = MockResponse(chunks)

    source = StreamingResponseSource(response, encoding=None)
    loop = asyncio.get_running_loop()

    await source.start_producer(loop)

    # Slow consumer
    def slow_consume():
        result = []
        for chunk in source.gen:
            time.sleep(0.001)
            result.append(chunk)
        return result

    result = await loop.run_in_executor(None, slow_consume)

    # All chunks should still be received despite slow consumer
    assert len(result) == 100
    assert result == chunks


@pytest.mark.asyncio
async def test_sync_close_runs_on_event_loop_thread():
    """close() from an executor thread runs response teardown on the loop thread."""
    loop_thread = threading.current_thread()
    close_thread = None

    class StalledContent:
        @staticmethod
        async def read(n=-1):
            await asyncio.sleep(3600)

    class StalledResponse:
        def __init__(self):
            self.content = StalledContent()
            self.headers = {}
            self.closed = False

        def close(self):
            nonlocal close_thread
            close_thread = threading.current_thread()
            self.closed = True

    response = StalledResponse()

    source = StreamingResponseSource(response, encoding=None)
    loop = asyncio.get_running_loop()
    await source.start_producer(loop)

    await loop.run_in_executor(None, source.close)

    for _ in range(100):
        if response.closed:
            break
        await asyncio.sleep(0.01)

    assert response.closed
    assert close_thread is loop_thread

    if source._producer_task is not None:
        try:
            await source._producer_task
        except asyncio.CancelledError:
            pass


class MockTransform:
    """Mock NativeTransform."""

    def __init__(self, chunks=None):
        self.chunks = chunks or [b"chunk1", b"chunk2"]

    def build_insert(self, context):
        yield from self.chunks


class FailingTransform:
    """Mock NativeTransform that raises error."""

    @staticmethod
    def build_insert(context):
        yield b"chunk1"
        raise ValueError("Serialization error")


class MockContext:
    """Mock InsertContext."""


@pytest.mark.asyncio
async def test_streaming_insert_basic():
    """Test basic streaming insert (reverse bridge)."""
    transform = MockTransform()
    context = MockContext()
    loop = asyncio.get_running_loop()

    source = StreamingInsertSource(transform, context, loop)
    source.start_producer()

    chunks = []
    async for chunk in source.async_generator():
        chunks.append(chunk)

    await source.close()

    assert chunks == [b"chunk1", b"chunk2"]


@pytest.mark.asyncio
async def test_streaming_insert_error_propagation():
    """Test that insert producer errors are propagated to async consumer."""
    transform = FailingTransform()
    context = MockContext()
    loop = asyncio.get_running_loop()

    source = StreamingInsertSource(transform, context, loop)
    source.start_producer()

    chunks = []
    with pytest.raises(ValueError, match="Serialization error"):
        async for chunk in source.async_generator():
            chunks.append(chunk)

    await source.close()

    # Should have received first chunk before error
    assert chunks == [b"chunk1"]


@pytest.mark.asyncio
async def test_streaming_insert_backpressure():
    """Test backpressure in streaming insert."""
    chunks = [f"chunk{i}".encode() for i in range(100)]
    transform = MockTransform(chunks)
    context = MockContext()
    loop = asyncio.get_running_loop()

    # Small queue size to force backpressure
    source = StreamingInsertSource(transform, context, loop, maxsize=2)
    source.start_producer()

    received = []
    async for chunk in source.async_generator():
        received.append(chunk)
        # Yield to allow producer to run (since we're in same loop/process)
        await asyncio.sleep(0.001)

    await source.close()

    assert len(received) == 100
    assert received == chunks


EXCEPTION_TAG = "fotmsnzgmwatfhfe"
SERVER_ERROR = "Code: 395. DB::Exception: boom: while executing 'FUNCTION throwIf'. (FUNCTION_THROW_IF_VALUE_IS_NON_ZERO)"


def tagged_exception_block(tag: str = EXCEPTION_TAG, message: str = SERVER_ERROR) -> bytes:
    """The in-band exception block a server appends after it has already committed a 200."""
    return f"\r\n__exception__\r\n{tag}\r\n{message}\n{len(message)} {tag}\r\n__exception__\r\n".encode()


class MockByteSource:
    """Minimal stand-in for a streaming response source consumed by StreamingFileAdapter."""

    def __init__(self, chunks, exception_tag=None, error=None):
        self.chunks = chunks
        self.exception_tag = exception_tag
        self.error = error

    @property
    def gen(self):
        def generator():
            yield from self.chunks
            if self.error:
                raise self.error

        return generator()


def read_all(adapter) -> bytes:
    """Drain the adapter the way PyArrow does, in bounded reads."""
    data = b""
    while True:
        chunk = adapter.read(1024)
        if not chunk:
            return data
        data += chunk


class TestExceptionTail:
    """The tail keeps only the trailing bytes, by reference, so a large result is not retained."""

    def test_retains_only_bounded_trailing_bytes(self):
        tail = _ExceptionTail()
        for _ in range(50):
            tail.add(b"x" * (_EXCEPTION_TAIL_SIZE // 4))
        message = tail.message
        assert len(message) < _EXCEPTION_TAIL_SIZE * 2
        assert message.endswith(b"x" * 100)

    def test_short_stream_is_retained_whole(self):
        tail = _ExceptionTail()
        tail.add(b"user_1")
        tail.add(b"user_2")
        assert tail.message == b"user_1user_2"

    def test_error_split_across_chunks_is_recovered(self):
        # The marker can straddle a chunk boundary, so the tail must be joined before scanning.
        block = tagged_exception_block()
        tail = _ExceptionTail()
        tail.add(b"\x00" * 79)
        tail.add(block[:13])
        tail.add(block[13:])
        assert block in tail.message


class TestArrowStreamExceptionDetection:
    """Arrow paths hand raw bytes to PyArrow, so a mid-stream server error has to be recovered
    from the response tail rather than from the parsed stream (issue #913)."""

    def test_transport_abort_with_tagged_block_reports_server_error(self):
        source = MockByteSource(
            [b"\x00" * 1024, tagged_exception_block()],
            exception_tag=EXCEPTION_TAG,
            error=OperationalError("Failed to read response data from server"),
        )
        adapter = StreamingFileAdapter(source)
        with pytest.raises(StreamFailureError, match="boom"):
            read_all(adapter)

    def test_transport_abort_without_tag_falls_back_to_code_prefix(self):
        # Servers older than the exception-tag protocol append the bare error text.
        source = MockByteSource(
            [b"\x00" * 1024, SERVER_ERROR.encode()],
            error=OperationalError("Failed to read response data from server"),
        )
        adapter = StreamingFileAdapter(source)
        with pytest.raises(StreamFailureError, match="boom"):
            read_all(adapter)

    def test_clean_eof_with_tagged_block_reports_server_error(self):
        source = MockByteSource([b"\x00" * 1024, tagged_exception_block()], exception_tag=EXCEPTION_TAG)
        adapter = StreamingFileAdapter(source)
        with pytest.raises(StreamFailureError, match="boom"):
            read_all(adapter)

    def test_clean_eof_without_tag_does_not_guess(self):
        # Arrow payloads are binary, so a bare `Code: ` match at a clean EOF is not trustworthy
        # evidence of a failure and must not turn a successful stream into an error.
        source = MockByteSource([b"\x00" * 13, SERVER_ERROR.encode()])
        adapter = StreamingFileAdapter(source)
        assert read_all(adapter) == b"\x00" * 13 + SERVER_ERROR.encode()

    def test_transport_abort_without_server_error_still_reports_stream_failure(self):
        source = MockByteSource([b"\x00" * 1024], error=OperationalError("Failed to read response data from server"))
        adapter = StreamingFileAdapter(source)
        with pytest.raises(StreamFailureError, match="connection closed by server"):
            read_all(adapter)

    def test_successful_stream_is_unchanged(self):
        chunks = [b"user_1", b"user_2", b"user_3"]
        adapter = StreamingFileAdapter(MockByteSource(chunks, exception_tag=EXCEPTION_TAG))
        assert read_all(adapter) == b"".join(chunks)

    def test_unrelated_error_is_not_rewritten(self):
        # Only transport failures are candidates for an in-band error; anything else is a bug that
        # must keep its own type and traceback.
        source = MockByteSource([b"\x00" * 13], error=ValueError("not a transport failure"))
        adapter = StreamingFileAdapter(source)
        with pytest.raises(ValueError, match="not a transport failure"):
            read_all(adapter)

    @pytest.mark.parametrize(
        "show_clickhouse_errors,expected",
        [(False, GENERIC_CLICKHOUSE_ERROR), ("scrub", "boom")],
    )
    def test_error_visibility_policy_is_applied(self, show_clickhouse_errors, expected):
        # The Arrow paths must honor the same show_clickhouse_errors policy as the native path.
        source = MockByteSource(
            [tagged_exception_block()],
            exception_tag=EXCEPTION_TAG,
            error=OperationalError("Failed to read response data from server"),
        )
        adapter = StreamingFileAdapter(source, show_clickhouse_errors)
        with pytest.raises(StreamFailureError, match=expected):
            read_all(adapter)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
