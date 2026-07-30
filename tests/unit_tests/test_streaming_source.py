import asyncio
import gzip
import time
import zlib
from unittest.mock import Mock

import lz4.frame
import pytest

from clickhouse_connect.driver.compression import _zstd_compress
from clickhouse_connect.driver.exceptions import OperationalError, StreamFailureError
from clickhouse_connect.driver.streaming import (
    StreamExceptionScanner,
    StreamingFileAdapter,
    StreamingInsertSource,
    StreamingResponseSource,
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


def _inband_exception(tag: str, message: str, sep: bytes = b"") -> bytes:
    """Build an in-band exception block. ``sep`` is the separator ClickHouse writes
    between the ``__exception__`` marker and the tag: older servers write them adjacent
    (``sep=b""``, the format captured in ``tests.helpers.TAGGED_EXCEPTION_BODY``), while
    server 26.5 separates them with CRLF (``sep=b"\\r\\n"``). The scanner must detect both.
    """
    body = message.encode()
    tb = tag.encode()
    return b"\r\n__exception__" + sep + tb + b"\r\n" + body + b"\r\n" + str(len(body)).encode() + b" " + tb + sep + b"__exception__\r\n"


def _chunks(data: bytes, size: int) -> list[bytes]:
    return [data[i : i + size] for i in range(0, len(data), size)] or [b""]


@pytest.mark.parametrize("sep", [b"", b"\r\n"], ids=["adjacent-tag", "crlf-tag"])
@pytest.mark.parametrize("chunk_size", [7, 64, 100000])
def test_scanner_extracts_and_holds_back_block(chunk_size, sep):
    """The scanner extracts the tagged server error and never forwards exception-block
    bytes to the consumer, across both marker layouts and any chunking."""
    tag = "abcd1234efgh5678"
    prefix = b"arrow-batch-bytes" * 40
    message = "Code: 395. DB::Exception: Value passed to 'throwIf' function is non-zero"
    stream = prefix + _inband_exception(tag, message, sep)

    scanner = StreamExceptionScanner(tag)
    forwarded = b"".join(scanner.feed(chunk) for chunk in _chunks(stream, chunk_size))
    forwarded += scanner.flush()

    assert scanner.armed is True
    assert scanner.error_message == message
    # The valid prefix is forwarded, but nothing from the exception block is.
    assert forwarded.startswith(prefix)
    assert b"__exception__" not in forwarded


def test_scanner_matches_repo_exception_fixture():
    """The scanner detects the canonical tagged-exception body the native-path tests
    use, so both paths agree on the wire format."""
    from tests.helpers import TAGGED_EXCEPTION_BODY, TAGGED_EXCEPTION_TAG

    scanner = StreamExceptionScanner(TAGGED_EXCEPTION_TAG)
    forwarded = b"".join(scanner.feed(c) for c in _chunks(TAGGED_EXCEPTION_BODY, 16)) + scanner.flush()

    assert scanner.armed is True
    assert scanner.error_message == "Big bam occurred right while reading the data"
    assert forwarded == b"bodybodybodybody\r\n"
    assert b"__exception__" not in forwarded


def test_scanner_ignores_foreign_tag():
    """A block tagged with a different (unknown) tag must not be treated as our
    exception, so legitimate result bytes are forwarded unchanged."""
    scanner = StreamExceptionScanner("our0tag0our0tag0")
    data = b"payload-bytes" + _inband_exception("other0tag0other0", "not our error")

    forwarded = b"".join(scanner.feed(c) for c in _chunks(data, 16)) + scanner.flush()

    assert scanner.armed is False
    assert scanner.error_message is None
    assert forwarded == data


def test_scanner_passthrough_without_exception():
    """A clean stream (no exception block) is forwarded byte-for-byte."""
    scanner = StreamExceptionScanner("tag0tag0tag0tag0")
    data = b"complete arrow stream with no server error" * 10

    forwarded = b"".join(scanner.feed(c) for c in _chunks(data, 13)) + scanner.flush()

    assert scanner.armed is False
    assert forwarded == data


def _read_all(adapter: StreamingFileAdapter, size: int = 32) -> bytes:
    out = b""
    while True:
        piece = adapter.read(size)
        if not piece:
            return out
        out += piece


def test_file_adapter_raises_streamfailure_on_inband_exception():
    """A mid-stream server exception in the byte stream surfaces as a clean
    StreamFailureError rather than being fed to the consumer as garbled data."""
    tag = "tag0tag0tag0tag0"
    payload = b"ARROWDATA" * 200
    block = _inband_exception(tag, "Code: 241. DB::Exception: Memory limit exceeded")
    adapter = StreamingFileAdapter(iter([payload[:150], payload[150:] + block]), exception_tag=tag)

    with pytest.raises(StreamFailureError, match="Memory limit exceeded"):
        _read_all(adapter)


def test_file_adapter_passthrough_without_tag():
    """Without an exception tag the adapter is a plain pass-through (unchanged
    behavior for servers that do not send the exception-tag header)."""
    adapter = StreamingFileAdapter(iter([b"abc", b"def", b"ghi"]))
    assert adapter.read(-1) == b"abcdefghi"


def test_file_adapter_clean_stream_with_tag_no_raise():
    """A clean stream is delivered intact even when exception detection is armed, and
    raise_if_pending is a no-op when no exception occurred."""
    tag = "tag0tag0tag0tag0"
    adapter = StreamingFileAdapter(iter([b"clean", b"arrow", b"data"]), exception_tag=tag)
    assert _read_all(adapter, 4) == b"cleanarrowdata"
    adapter.raise_if_pending()


def test_file_adapter_raise_if_pending_surfaces_exception():
    """If the consumer stops reading before reaching the exception block (for example
    an Arrow reader treating trailing padding as end-of-stream), raise_if_pending still
    surfaces the server error."""
    tag = "tag0tag0tag0tag0"
    block = _inband_exception(tag, "Code: 159. DB::Exception: Timeout exceeded")
    adapter = StreamingFileAdapter(iter([b"validprefix", block]), exception_tag=tag)

    assert adapter.read(11) == b"validprefix"
    with pytest.raises(StreamFailureError, match="Timeout exceeded"):
        adapter.raise_if_pending()


def test_file_adapter_transport_abort_prefers_inband_exception():
    """When the transport aborts after the server wrote an in-band error, the clean
    server error is preferred over the raw transport error."""
    tag = "tag1tag1tag1tag1"
    block = _inband_exception(tag, "Code: 210. DB::Exception: Connection reset")

    def gen():
        yield b"arrowdata" * 30
        yield block
        raise ConnectionError("raw transport failure")

    adapter = StreamingFileAdapter(gen(), exception_tag=tag)
    with pytest.raises(StreamFailureError, match="Connection reset"):
        _read_all(adapter)


def test_file_adapter_transport_abort_without_inband_reraises():
    """A transport abort with no in-band error is re-raised as-is (not masked)."""
    tag = "tag2tag2tag2tag2"

    def gen():
        yield b"arrowdata" * 30
        raise ConnectionError("raw transport failure")

    adapter = StreamingFileAdapter(gen(), exception_tag=tag)
    with pytest.raises(ConnectionError, match="raw transport failure"):
        _read_all(adapter)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
