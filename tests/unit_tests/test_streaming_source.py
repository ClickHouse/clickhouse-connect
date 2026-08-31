import asyncio
import gzip
import threading
import time
import zlib
from contextlib import suppress
from unittest.mock import Mock

import lz4.frame
import pytest

from clickhouse_connect.driver._backend.http_async import SessionLease, _one_shot
from clickhouse_connect.driver.compression import _zstd_compress
from clickhouse_connect.driver.exceptions import OperationalError
from clickhouse_connect.driver.streaming import (
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


@pytest.mark.asyncio
async def test_sync_close_releases_session_lease_on_event_loop_thread():
    class FakeSession:
        def __init__(self):
            self.closed = False

        async def close(self):
            self.closed = True

    class StalledContent:
        @staticmethod
        async def read(n=-1):
            await asyncio.Event().wait()

    class StalledResponse:
        def __init__(self, release):
            self.content = StalledContent()
            self.headers = {}
            self.closed = False
            self._lease_release = _one_shot(release)

        def close(self):
            self.closed = True

    loop = asyncio.get_running_loop()
    previous_debug = loop.get_debug()
    loop.set_debug(True)
    session = FakeSession()
    lease = SessionLease(session)
    lease.acquire()
    response = StalledResponse(lease.release)
    source = StreamingResponseSource(response)
    await source.start_producer(loop)
    close_started = asyncio.Event()

    async def close_session():
        close_started.set()
        await lease.wait_drained()
        await session.close()

    close_task = asyncio.create_task(close_session())
    await asyncio.wait_for(close_started.wait(), timeout=1)
    assert not close_task.done()
    try:
        await loop.run_in_executor(None, source.close)
        await asyncio.wait_for(close_task, timeout=1)
    finally:
        loop.set_debug(previous_debug)
        await source.aclose()
        if not close_task.done():
            close_task.cancel()
            with suppress(asyncio.CancelledError):
                await close_task

    assert lease._inflight == 0
    assert session.closed
    assert response.closed


def test_release_lease_queues_on_stopped_open_event_loop():
    release_threads = []
    worker_threads = []
    response = MockResponse([])
    response._lease_release = lambda: release_threads.append(threading.get_ident())
    source = StreamingResponseSource(response)
    loop = asyncio.new_event_loop()
    source._loop = loop

    def release_from_worker():
        worker_threads.append(threading.get_ident())
        source._release_lease()

    worker = threading.Thread(target=release_from_worker)
    try:
        worker.start()
        worker.join(timeout=1)
        assert not worker.is_alive()
        assert release_threads == []

        loop.run_until_complete(asyncio.sleep(0))

        assert release_threads == [threading.get_ident()]
        assert release_threads != worker_threads
    finally:
        loop.close()


@pytest.mark.parametrize("failure", ["queue", "response"])
def test_sync_close_releases_lease_when_cleanup_fails(failure):
    cleanup_error = RuntimeError(f"{failure} cleanup failed")
    release_lease = Mock()
    response = MockResponse([])
    response._lease_release = _one_shot(release_lease)
    source = StreamingResponseSource(response)
    if failure == "queue":
        source.queue.shutdown = Mock(side_effect=cleanup_error)
    else:
        response.close = Mock(side_effect=cleanup_error)

    with pytest.raises(RuntimeError) as caught:
        source.close()

    assert caught.value is cleanup_error
    if failure == "queue":
        assert response.closed
    release_lease.assert_called_once_with()


@pytest.mark.asyncio
async def test_async_close_releases_lease_when_response_close_fails():
    close_error = RuntimeError("response close failed")
    release_lease = Mock()

    class FailingCloseResponse:
        content = MockContent([])
        headers = {}
        closed = False

        @staticmethod
        def close():
            raise close_error

    source = StreamingResponseSource(FailingCloseResponse())
    source.response._lease_release = _one_shot(release_lease)

    with pytest.raises(RuntimeError) as caught:
        await source.aclose()

    assert caught.value is close_error
    release_lease.assert_called_once_with()


@pytest.mark.asyncio
async def test_producer_releases_lease_when_queue_shutdown_fails():
    shutdown_error = RuntimeError("queue shutdown failed")
    release_lease = Mock()
    response = MockResponse([])
    response._lease_release = _one_shot(release_lease)
    source = StreamingResponseSource(response)
    source.queue.shutdown = Mock(side_effect=shutdown_error)

    await source.start_producer(asyncio.get_running_loop())
    with pytest.raises(RuntimeError) as caught:
        await source._producer_task

    assert caught.value is shutdown_error
    release_lease.assert_called_once_with()


@pytest.mark.asyncio
async def test_async_close_releases_lease_when_queue_shutdown_fails():
    shutdown_error = RuntimeError("queue shutdown failed")
    release_lease = Mock()
    response = MockResponse([])
    response._lease_release = _one_shot(release_lease)
    source = StreamingResponseSource(response)
    source.queue.shutdown = Mock(side_effect=shutdown_error)

    with pytest.raises(RuntimeError) as caught:
        await source.aclose()

    assert caught.value is shutdown_error
    assert response.closed
    release_lease.assert_called_once_with()


@pytest.mark.asyncio
async def test_async_close_releases_lease_when_cleanup_is_cancelled():
    close_started = asyncio.Event()
    release_lease = Mock()

    class SlowCloseResponse:
        content = MockContent([])
        headers = {}
        closed = False

        def close(self):
            self.closed = True
            close_started.set()

    response = SlowCloseResponse()
    response._lease_release = _one_shot(release_lease)
    source = StreamingResponseSource(response)
    close_task = asyncio.create_task(source.aclose())
    await asyncio.wait_for(close_started.wait(), timeout=1)

    close_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await close_task
    release_lease.assert_called_once_with()
    await source.aclose()

    release_lease.assert_called_once_with()


@pytest.mark.asyncio
async def test_async_close_tears_down_response_when_producer_wait_is_cancelled():
    release_lease = Mock()
    response = MockResponse([])
    response._lease_release = _one_shot(release_lease)
    source = StreamingResponseSource(response)
    producer_cancelled = asyncio.Event()
    producer_release = asyncio.Event()

    async def slow_cancel_producer():
        try:
            await asyncio.Event().wait()
        finally:
            producer_cancelled.set()
            await producer_release.wait()

    producer_task = asyncio.create_task(slow_cancel_producer())
    source._producer_task = producer_task
    close_task = asyncio.create_task(source.aclose())
    await asyncio.wait_for(producer_cancelled.wait(), timeout=1)

    close_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await close_task

    assert response.closed
    release_lease.assert_called_once_with()
    producer_release.set()
    with pytest.raises(asyncio.CancelledError):
        await producer_task


@pytest.mark.asyncio
async def test_async_close_suppresses_ordinary_producer_task_error(caplog):
    producer_started = asyncio.Event()
    producer_error = RuntimeError("producer cleanup failed")
    release_lease = Mock()
    response = MockResponse([])
    response._lease_release = _one_shot(release_lease)
    source = StreamingResponseSource(response)

    async def fail_on_cancel():
        producer_started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            raise producer_error from None

    source._loop = asyncio.get_running_loop()
    source._producer_task = asyncio.create_task(fail_on_cancel())
    await asyncio.wait_for(producer_started.wait(), timeout=1)

    with caplog.at_level("DEBUG", logger="clickhouse_connect.driver.streaming"):
        await source.aclose()

    assert response.closed
    release_lease.assert_called_once_with()
    assert "Discarded producer error during streaming response cleanup" in caplog.messages


@pytest.mark.asyncio
async def test_async_close_retrieves_late_producer_error_when_wait_is_cancelled(caplog):
    producer_cancelled = asyncio.Event()
    producer_release = asyncio.Event()
    producer_error = RuntimeError("late producer cleanup failed")
    response = MockResponse([])
    source = StreamingResponseSource(response)

    async def fail_after_cancel():
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            producer_cancelled.set()
            await producer_release.wait()
            raise producer_error from None

    producer_task = asyncio.create_task(fail_after_cancel())
    source._producer_task = producer_task
    with caplog.at_level("DEBUG", logger="clickhouse_connect.driver.streaming"):
        close_task = asyncio.create_task(source.aclose())
        await asyncio.wait_for(producer_cancelled.wait(), timeout=1)
        close_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await close_task

        producer_release.set()
        loop = asyncio.get_running_loop()
        deadline = loop.time() + 1
        while "Discarded producer error during streaming response cleanup" not in caplog.messages:
            if loop.time() >= deadline:
                raise TimeoutError("producer task result was not retrieved")
            await asyncio.sleep(0.001)

    assert producer_task.done()
    assert producer_task.exception() is producer_error
    assert response.closed


class MockTransform:
    """Mock NativeTransform."""

    def __init__(self, chunks=None):
        self.chunks = chunks or [b"chunk1", b"chunk2"]

    def build_insert(self, context, error_handler=None):
        yield from self.chunks


class FailingTransform:
    """Mock NativeTransform that raises error."""

    @staticmethod
    def build_insert(context, error_handler=None):
        yield b"chunk1"
        raise ValueError("Serialization error")


class MockContext:
    """Mock InsertContext."""


class BackpressuredTransform:
    def __init__(self):
        self.blocked_put_started = threading.Event()
        self.finished = threading.Event()

    def build_insert(self, context, error_handler=None):
        try:
            yield b"chunk1"
            yield b"chunk2"
            self.blocked_put_started.set()
            yield b"chunk3"
        finally:
            self.finished.set()


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


@pytest.mark.asyncio
async def test_streaming_insert_generator_close_unblocks_backpressured_producer(caplog):
    transform = BackpressuredTransform()
    source = StreamingInsertSource(transform, MockContext(), asyncio.get_running_loop(), maxsize=1)
    source.start_producer()
    generator = source.async_generator()

    try:
        assert await generator.__anext__() == b"chunk1"
        assert await asyncio.get_running_loop().run_in_executor(None, transform.blocked_put_started.wait, 1)

        await asyncio.wait_for(generator.aclose(), timeout=1)

        assert transform.finished.is_set()
        assert source._producer_future.done()
        with pytest.raises(RuntimeError, match="shutdown"):
            source.queue.sync_q.put(b"late chunk")
        await source.close()
        assert "Insert producer error" not in caplog.messages
    finally:
        source.queue.shutdown()
        await source.close()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
