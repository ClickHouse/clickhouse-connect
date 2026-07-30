import asyncio
import gzip
import time
import zlib
from unittest.mock import Mock

import lz4.frame
import pytest

from clickhouse_connect.driver.compression import _zstd_compress
from clickhouse_connect.driver.exceptions import OperationalError
from clickhouse_connect.driver.streaming import (
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


# ---------------------------------------------------------------------------
# StreamingFileAdapter: PyArrow file-like reader over the streamed chunks.
# ---------------------------------------------------------------------------


def _pattern(n: int) -> bytes:
    """Deterministic, non-repeating byte payload of length n."""
    return bytes((i * 197 + 13) & 0xFF for i in range(n))


_THRESHOLD = StreamingFileAdapter.COMPACT_THRESHOLD
_BIG = _pattern(_THRESHOLD * 2 + 8192)  # built once, sliced by the large-stream tests


class _ChunkSource:
    """Minimal stand-in for StreamingResponseSource: exposes a .gen iterator
    over a fixed list of byte chunks, the same contract StreamingFileAdapter
    consumes from the real streaming source."""

    def __init__(self, chunks):
        self._gen = iter(chunks)

    @property
    def gen(self):
        return self._gen


def _read_in_steps(adapter, size):
    """Drain the adapter with repeated read(size) calls until EOF, returning
    the concatenation. Also asserts no read ever overruns the requested size."""
    out = bytearray()
    while True:
        data = adapter.read(size)
        assert size == -1 or len(data) <= size
        if not data:
            break
        out += data
    return bytes(out)


@pytest.mark.parametrize(
    "chunks",
    [
        [],
        [b""],
        [b"single"],
        [_pattern(500)],
        [b"ab", b"cde", b"f", b"ghij"],
        [_pattern(300) for _ in range(20)],
    ],
)
@pytest.mark.parametrize("size", [1, 3, 7, 8, 64, 250, 4096, 100_000])
def test_file_adapter_fixed_size_reads_reconstruct_stream(chunks, size):
    """read(size) called repeatedly returns exactly the concatenated stream,
    however the source chunk boundaries line up with the read size."""
    expected = b"".join(chunks)
    adapter = StreamingFileAdapter(_ChunkSource(list(chunks)))
    assert _read_in_steps(adapter, size) == expected


@pytest.mark.parametrize(
    "chunks",
    [
        [],
        [_pattern(500)],
        [b"ab", b"cde", b"f", b"ghij"],
        [_BIG[i : i + 256 * 1024] for i in range(0, _THRESHOLD * 2, 256 * 1024)],
    ],
)
def test_file_adapter_read_all_returns_whole_stream(chunks):
    """read(-1) returns the entire remaining stream in a single call."""
    expected = b"".join(chunks)
    adapter = StreamingFileAdapter(_ChunkSource(list(chunks)))
    assert adapter.read(-1) == expected
    assert adapter.read(-1) == b""


@pytest.mark.parametrize(
    "chunks",
    [
        [_BIG[: _THRESHOLD + 5000]],  # one chunk larger than the threshold -> in-chunk compaction
        [_BIG[i : i + 256 * 1024] for i in range(0, _THRESHOLD * 2, 256 * 1024)],  # multi-chunk past threshold
    ],
)
@pytest.mark.parametrize("size", [64, 1000, 100_000])
def test_file_adapter_large_stream_reconstructs(chunks, size):
    """Streams larger than the compaction threshold are reassembled byte-exact
    across the compaction boundary."""
    expected = b"".join(chunks)
    adapter = StreamingFileAdapter(_ChunkSource(list(chunks)))
    assert _read_in_steps(adapter, size) == expected


def test_file_adapter_size_larger_than_stream():
    payload = _pattern(1000)
    adapter = StreamingFileAdapter(_ChunkSource([payload]))
    assert adapter.read(1_000_000) == payload
    assert adapter.read(1_000_000) == b""


def test_file_adapter_reads_after_eof_and_close_return_empty():
    adapter = StreamingFileAdapter(_ChunkSource([b"data"]))
    assert adapter.read(2) == b"da"
    assert adapter.read(10) == b"ta"
    assert adapter.read(10) == b""  # source exhausted
    adapter.close()
    assert adapter.read(10) == b""


def test_file_adapter_zero_size_and_read_all_after_partial():
    """read(0) returns b"" with or without a buffered residual, and read(-1)
    drains whatever residual remains after a partial read."""
    adapter = StreamingFileAdapter(_ChunkSource([b"abcdef"]))
    assert adapter.read(0) == b""  # nothing buffered yet
    assert adapter.read(3) == b"abc"  # buffers the chunk, consumes part
    assert adapter.read(0) == b""  # residual present, still an empty read
    assert adapter.read(-1) == b"def"  # unbounded read returns the remainder
    assert adapter.read(-1) == b""


def test_file_adapter_empty_chunk_terminates_stream():
    """A falsy chunk from the source signals end-of-stream, matching the
    original adapter behavior."""
    adapter = StreamingFileAdapter(_ChunkSource([b"before", b"", b"after"]))
    assert adapter.read(-1) == b"before"
    assert adapter.read(-1) == b""


def test_file_adapter_small_reads_do_not_recopy_residual():
    """Regression: the pre-fix adapter rebuilt the residual on every read
    (self.buffer = self.buffer[size:]), so N small reads over one buffered
    chunk cost O(N * residual) copying. The fix keeps a single backing buffer
    and advances an integer offset, so the buffer is (re)allocated only a
    handful of times rather than once per read."""
    payload = _pattern(64 * 1024)  # < COMPACT_THRESHOLD, so no compaction fires
    adapter = StreamingFileAdapter(_ChunkSource([payload]))
    out = bytearray()
    prev = None
    reallocations = 0
    while True:
        data = adapter.read(8)
        if not data:
            break
        out += data
        current = adapter.buffer  # prev stays referenced -> no address reuse
        if current is not prev:
            reallocations += 1
            prev = current
    assert bytes(out) == payload
    # 64 KiB read 8 bytes at a time is 8192 reads; the old code reallocated the
    # residual on essentially every one of them.
    assert reallocations <= 4


def test_file_adapter_offset_is_compacted_on_large_stream():
    """The advancing read offset is periodically reset by compaction, so it
    cannot grow without bound on a stream far larger than the threshold."""
    payload = _BIG[: _THRESHOLD * 2 + 777]
    adapter = StreamingFileAdapter(_ChunkSource([payload]))
    out = bytearray()
    max_pos = 0
    while True:
        data = adapter.read(50)
        if not data:
            break
        out += data
        max_pos = max(max_pos, adapter.pos)
    assert bytes(out) == payload
    # Without compaction the offset would climb to ~2 * threshold.
    assert max_pos < _THRESHOLD


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
