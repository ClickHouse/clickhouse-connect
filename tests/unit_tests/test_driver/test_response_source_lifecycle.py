import asyncio
import gc
import http.client
import io
import subprocess
import sys
import threading
from concurrent.futures import ThreadPoolExecutor
from textwrap import dedent
from unittest.mock import AsyncMock, Mock, patch

import pytest
from urllib3.exceptions import HTTPError
from urllib3.response import HTTPResponse

from clickhouse_connect.driver import httputil
from clickhouse_connect.driver.buffer import ResponseBuffer
from clickhouse_connect.driver.exceptions import OperationalError
from clickhouse_connect.driver.httputil import ResponseSource
from clickhouse_connect.driver.query import QueryResult
from clickhouse_connect.driver.streaming import ReadAheadSource, StreamingResponseSource


@pytest.fixture(params=["python", "cython"])
def buffer_cls(request):
    if request.param == "cython":
        return pytest.importorskip("clickhouse_connect.driverc.buffer").ResponseBuffer
    return ResponseBuffer


@pytest.mark.parametrize("read_ahead", [False, True])
@pytest.mark.parametrize("early_chunks", [0, 1, 2])
def test_close_waits_for_transport_reader(read_ahead, early_chunks):
    reading = threading.Event()
    release_read = threading.Event()
    close_started = threading.Event()
    closed = threading.Event()
    overlapping_cleanup = []
    response = Mock(headers={})

    def chunks(*_args):
        for _ in range(early_chunks):
            yield b"early"
        reading.set()
        try:
            assert release_read.wait(5)
        finally:
            reading.clear()
        yield b"late"
        yield b"last"

    def record_cleanup():
        overlapping_cleanup.append(reading.is_set())

    response.stream = chunks
    response.drain_conn.side_effect = record_cleanup
    response.close.side_effect = record_cleanup
    with patch.object(httputil.common, "get_setting", return_value=0):
        source = ResponseSource(response)
    owner = ReadAheadSource(source) if read_ahead else source
    consumer = owner.gen
    for _ in range(early_chunks):
        assert next(consumer) == b"early"

    def close():
        close_started.set()
        try:
            owner.close()
        finally:
            closed.set()

    with ThreadPoolExecutor(max_workers=3) as executor:
        read = None
        if not read_ahead or early_chunks < 2:
            read = executor.submit(next, consumer, None)
        try:
            assert reading.wait(1)
            cleanup = executor.submit(close)
            assert close_started.wait(1)
            # The producer must still own the read after the old one-second join expires.
            wait = 1.2 if read_ahead and early_chunks == 2 else 0.1
            assert not closed.wait(wait)
            response.close.assert_not_called()
        finally:
            release_read.set()
            if read is not None:
                read.result(timeout=2)
            if close_started.is_set():
                cleanup.result(timeout=2)
            owner.close()
    assert not any(overlapping_cleanup)
    response.close.assert_called_once_with()


def test_concurrent_response_close_waits_for_cleanup():
    draining = threading.Event()
    release_drain = threading.Event()
    competing_close = threading.Event()
    response = Mock(headers={})

    def chunks(*_args):
        yield b"first"
        draining.set()
        assert release_drain.wait(3)
        yield b"last"

    response.stream = chunks
    with patch.object(httputil.common, "get_setting", return_value=0):
        source = ResponseSource(response)
    assert next(source.gen) == b"first"

    def close_again():
        competing_close.set()
        source.close()

    with ThreadPoolExecutor(max_workers=2) as executor:
        first_close = executor.submit(source.close)
        second_close = None
        try:
            assert draining.wait(1)
            second_close = executor.submit(close_again)
            assert competing_close.wait(1)
            assert not first_close.done()
            assert not second_close.done()
            response.close.assert_not_called()
        finally:
            release_drain.set()
            first_close.result(timeout=2)
            if second_close is not None:
                second_close.result(timeout=2)
    source.close()
    response.close.assert_called_once_with()


@pytest.mark.parametrize("read_fails", [False, True])
def test_close_on_reader_thread_defers_cleanup(read_fails):
    response = Mock(headers={})
    order = []

    def chunks(*_args):
        try:
            order.append("reading")
            source.close()
            source.close()
            response.close.assert_not_called()
            if read_fails:
                raise OSError("read failed")
            yield b"first"
            order.append("draining")
            source.close()
            yield b"last"
        finally:
            order.append("read finished")

    def close_response():
        order.append("closed")
        source.close()

    response.stream = chunks
    response.close.side_effect = close_response
    with patch.object(httputil.common, "get_setting", return_value=0):
        source = ResponseSource(response)
    errors = []

    def read():
        try:
            assert list(source.gen) == [b"first"]
        except BaseException as ex:
            errors.append(ex)

    reader = threading.Thread(target=read, daemon=True)
    reader.start()
    reader.join(2)
    assert not reader.is_alive(), "close() deadlocked on its own reader"
    if read_fails:
        assert len(errors) == 1
        assert isinstance(errors[0], OperationalError)
        assert isinstance(errors[0].__cause__, OSError)
    else:
        assert not errors
    assert order == ["reading", *([] if read_fails else ["draining"]), "read finished", "closed"]
    source.close()
    response.close.assert_called_once_with()


def test_abandoned_stream_collected_on_producer_thread(buffer_cls):
    # A deadlock during cyclic GC can disable later collections process-wide.
    code = dedent("""
        import gc
        import importlib
        import sys
        import threading
        import weakref
        from unittest.mock import Mock, patch
        from clickhouse_connect.driver import httputil
        from clickhouse_connect.driver.httputil import ResponseSource
        from clickhouse_connect.driver.streaming import ReadAheadSource

        buffer_cls = importlib.import_module(sys.argv[1]).ResponseBuffer
        trigger = threading.Event()
        def chunks(*_args):
            yield b"first"
            yield b"second"
            assert trigger.wait(5)
            gc.collect(0)
            yield b"third"
            yield b"last"

        gc.disable()
        response = Mock(headers={})
        response.stream = chunks
        with patch.object(httputil.common, "get_setting", return_value=0):
            source = ResponseSource(response)
        source_ref = weakref.ref(source)
        owner = ReadAheadSource(buffer_cls(source))
        owner_ref = weakref.ref(owner)
        del source
        consumer = owner.gen
        assert next(consumer) == b"first"
        assert next(consumer) == b"second"
        producer = owner._thread
        gc.collect()
        holder = {"owner": owner}
        holder["cycle"] = holder
        del owner, consumer, holder
        trigger.set()
        producer.join(2)
        assert not producer.is_alive(), "producer deadlocked during garbage collection"
        gc.collect()
        assert owner_ref() is None
        assert source_ref() is None
        response.close.assert_called_once_with()
    """)
    result = subprocess.run(
        [sys.executable, "-c", code, buffer_cls.__module__],
        capture_output=True,
        text=True,
        timeout=10,
        check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr


def test_abandoned_stream_collected_inside_producer_queue_put(buffer_cls):
    # Queue.put holds the queue mutex while a collection on the producer thread finalizes the owner.
    code = dedent("""
        import gc
        import importlib
        import sys
        import threading
        import time
        from unittest.mock import Mock, patch
        from clickhouse_connect.driver import httputil
        from clickhouse_connect.driver.httputil import ResponseSource
        from clickhouse_connect.driver.streaming import ReadAheadSource

        buffer_cls = importlib.import_module(sys.argv[1]).ResponseBuffer
        def chunks(*_args):
            for _ in range(100):
                yield b"x" * 64

        gc.disable()
        response = Mock(headers={})
        response.stream = chunks
        with patch.object(httputil.common, "get_setting", return_value=0):
            source = ResponseSource(response)
        owner = ReadAheadSource(buffer_cls(source), maxsize=4)
        del source
        source_queue = owner.queue
        consumer = owner.gen
        next(consumer)
        next(consumer)
        producer = owner._thread
        deadline = time.monotonic() + 5
        while not source_queue.full():
            assert time.monotonic() < deadline, "producer never filled the queue"
            time.sleep(0.01)
        gc.collect()

        armed = threading.Event()
        qsize = type(source_queue)._qsize
        def collect_in_put(queue):
            if armed.is_set() and threading.current_thread() is producer:
                armed.clear()
                gc.collect()
            return qsize(queue)
        source_queue._qsize = collect_in_put.__get__(source_queue)
        holder = {"owner": owner}
        holder["cycle"] = holder
        del owner, consumer, holder
        armed.set()
        producer.join(2)
        assert not armed.is_set(), "producer never collected inside Queue.put"
        assert not producer.is_alive(), "producer deadlocked during garbage collection"
        response.close.assert_called_once_with()
    """)
    result = subprocess.run(
        [sys.executable, "-c", code, buffer_cls.__module__],
        capture_output=True,
        text=True,
        timeout=10,
        check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr


def test_read_ahead_async_close_after_executor_shutdown(buffer_cls):
    release = threading.Event()

    def chunks(*_args):
        yield b"first"
        yield b"second"
        release.wait(5)
        yield b"last"

    response = Mock(headers={})
    response.stream = chunks
    with patch.object(httputil.common, "get_setting", return_value=0):
        source = ResponseSource(response)
    owner = ReadAheadSource(buffer_cls(source))
    consumer = owner.gen
    assert next(consumer) == b"first"
    assert next(consumer) == b"second"
    producer = owner._thread
    assert producer is not None

    async def run():
        await asyncio.get_running_loop().shutdown_default_executor()
        release.set()
        await owner.aclose()

    asyncio.run(run())
    response.close.assert_called_once_with()
    producer.join(2)
    assert not producer.is_alive()


@pytest.mark.parametrize("close_fails", [False, True])
def test_async_close_after_executor_shutdown(buffer_cls, close_fails):
    response = Mock(headers={}, stream=Mock(return_value=iter([b"first", b"last"])))
    close_error = RuntimeError("close failed")
    if close_fails:
        response.close.side_effect = close_error
    source = ResponseSource(response)
    buffer = buffer_cls(source)

    async def run():
        await asyncio.get_running_loop().shutdown_default_executor()
        if close_fails:
            with pytest.raises(RuntimeError) as excinfo:
                await buffer.aclose()
            assert excinfo.value is close_error
        else:
            await buffer.aclose()

    asyncio.run(run())
    response.close.assert_called_once_with()
    buffer.close()
    source.close()
    response.close.assert_called_once_with()


@pytest.mark.asyncio
@pytest.mark.parametrize("cancelled", [False, True])
async def test_external_close_keeps_cleanup_error(cancelled):
    reading = threading.Event()
    release_read = threading.Event()
    close_started = threading.Event()
    response = Mock(headers={})
    close_error = RuntimeError("close failed")
    read_lock = threading.RLock()

    class ObservedLock:
        def __enter__(self):
            if not read_lock.acquire(blocking=False):
                close_started.set()
                read_lock.acquire()

        def __exit__(self, *_args):
            read_lock.release()

    def chunks(*_args):
        reading.set()
        assert release_read.wait(3)
        yield b"first"
        yield b"last"

    response.stream = chunks
    response.close.side_effect = close_error
    with (
        patch.object(httputil.common, "get_setting", return_value=0),
        patch.object(httputil.threading, "RLock", return_value=ObservedLock()),
    ):
        source = ResponseSource(response)
    reader = asyncio.create_task(asyncio.to_thread(list, source.gen))
    assert await asyncio.to_thread(reading.wait, 1)
    cleanup = asyncio.create_task(source.aclose())
    try:
        assert await asyncio.to_thread(close_started.wait, 1)
        if cancelled:
            cleanup.cancel()
            await asyncio.sleep(0)
        assert not cleanup.done()
        response.close.assert_not_called()
    finally:
        release_read.set()
        assert await reader == [b"first"]
        with pytest.raises(asyncio.CancelledError if cancelled else RuntimeError) as excinfo:
            await cleanup
    assert (excinfo.value.__cause__ if cancelled else excinfo.value) is close_error
    response.close.assert_called_once_with()


@pytest.mark.parametrize("error_type", [HTTPError, OSError, http.client.HTTPException, RuntimeError])
@pytest.mark.parametrize("close_fails", [False, True])
def test_cleanup_errors_release_response_once(error_type, close_fails):
    response = Mock(headers={})
    drain_error = error_type("drain failed")
    close_error = ValueError("close failed")

    def chunks(*_args):
        yield b"first"
        raise drain_error

    response.stream = chunks
    if close_fails:
        response.close.side_effect = close_error
    with patch.object(httputil.common, "get_setting", return_value=0):
        source = ResponseSource(response)
    assert next(source.gen) == b"first"
    expected = close_error if close_fails else drain_error if error_type is RuntimeError else None
    if expected is not None:
        with pytest.raises(type(expected)) as excinfo:
            source.close()
        assert excinfo.value is expected
    else:
        source.close()
    source.close()
    response.close.assert_called_once_with()


@pytest.mark.parametrize("chunk_size", [4, 8])
def test_close_finishes_partial_http_chunk(chunk_size):
    wire = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n8\r\nabcdefgh\r\n4\r\nijkl\r\n0\r\n\r\n"

    class Body(io.BytesIO):
        consumed = 0

        def close(self):
            if not self.closed:
                self.consumed = self.tell()
            super().close()

    body = Body(wire)

    class Socket:
        def makefile(self, *_args):
            return body

    original = http.client.HTTPResponse(Socket(), method="POST")
    original.begin()
    pool = Mock()
    connection = Mock()
    response = HTTPResponse(
        body=original,
        headers=dict(original.headers),
        original_response=original,
        preload_content=False,
        pool=pool,
        connection=connection,
    )
    with patch.object(httputil.common, "get_setting", return_value=0):
        source = ResponseSource(response, chunk_size=chunk_size)
    assert next(source.gen) == b"abcdefgh"[:chunk_size]
    source.close()
    assert body.consumed == len(wire)
    assert original.isclosed()
    pool._put_conn.assert_called_once_with(connection)
    connection.close.assert_not_called()


@pytest.mark.asyncio
async def test_read_ahead_async_close_keeps_loop_running_during_direct_read(buffer_cls):
    reading = threading.Event()
    release_read = threading.Event()
    response = Mock(headers={})

    def chunks(*_args):
        reading.set()
        assert release_read.wait(3)
        yield b"late"

    response.stream = chunks
    with patch.object(httputil.common, "get_setting", return_value=0):
        source = ResponseSource(response)
    owner = ReadAheadSource(buffer_cls(source))
    read = asyncio.create_task(asyncio.to_thread(next, owner.gen, None))
    assert await asyncio.to_thread(reading.wait, 1)
    cleanup = asyncio.create_task(owner.aclose())
    try:
        await asyncio.sleep(0.05)
        assert not cleanup.done()
        assert not release_read.is_set()
        response.close.assert_not_called()
        cleanup.cancel()
        await asyncio.sleep(0)
        assert not cleanup.done()
    finally:
        release_read.set()
        await read
        with pytest.raises(asyncio.CancelledError):
            await cleanup
    response.close.assert_called_once_with()


def test_async_close_releases_buffered_read_with_busy_executor(buffer_cls):
    async def run():
        loop = asyncio.get_running_loop()
        loop.set_default_executor(ThreadPoolExecutor(max_workers=1))
        reading = asyncio.Event()
        response = Mock(closed=False)
        reads = 0

        async def read(_size):
            nonlocal reads
            reads += 1
            if reads == 1:
                return b"first"
            await asyncio.Event().wait()

        response.content.read = read
        response.close.side_effect = lambda: setattr(response, "closed", True)
        source = StreamingResponseSource(response)
        await source.start_producer(loop)
        owner = ReadAheadSource(buffer_cls(source))
        consumer = owner.gen
        assert await loop.run_in_executor(None, next, consumer, None) == b"first"

        def read_second():
            loop.call_soon_threadsafe(reading.set)
            return next(consumer, None)

        pending_read = loop.run_in_executor(None, read_second)
        await reading.wait()
        assert owner._thread is None
        cleanup = asyncio.create_task(owner.aclose())
        try:
            done, _ = await asyncio.wait((cleanup,), timeout=0.5)
            assert cleanup in done, "Cleanup waits behind the read it must release"
        finally:
            # Release a failed test without hanging executor shutdown.
            await source.aclose()
            await cleanup
            assert await pending_read is None
        response.close.assert_called_once_with()

    asyncio.run(run())


@pytest.mark.asyncio
@pytest.mark.parametrize("close_fails", [False, True])
async def test_cancelled_query_close_finishes_sync_drain(buffer_cls, close_fails):
    draining = asyncio.Event()
    release_drain = threading.Event()
    loop = asyncio.get_running_loop()
    response = Mock(headers={})
    close_error = ValueError("close failed")

    def chunks(*_args):
        yield b"first"
        loop.call_soon_threadsafe(draining.set)
        assert release_drain.wait(3)
        yield b"last"

    response.stream = chunks
    if close_fails:
        response.close.side_effect = close_error
    with patch.object(httputil.common, "get_setting", return_value=0):
        source = ResponseSource(response)
    assert next(source.gen) == b"first"
    result = QueryResult(source=buffer_cls(source))
    cleanup = asyncio.create_task(result.aclose())
    await asyncio.wait_for(draining.wait(), timeout=1)
    try:
        for _ in range(2):
            cleanup.cancel()
            await asyncio.sleep(0)
            assert not cleanup.done()
            response.close.assert_not_called()
    finally:
        release_drain.set()
        with pytest.raises(asyncio.CancelledError) as excinfo:
            await cleanup
    assert excinfo.value.__cause__ is (close_error if close_fails else None)
    source.close()
    response.close.assert_called_once_with()


@pytest.mark.asyncio
@pytest.mark.parametrize("error_type", [ValueError, asyncio.CancelledError])
async def test_buffer_does_not_repeat_failed_async_cleanup(buffer_cls, error_type):
    cleanup_error = error_type("close failed")
    source = Mock(gen=iter(()), exception_tag=None, aclose=AsyncMock(side_effect=cleanup_error))
    buffer = buffer_cls(source)
    with pytest.raises(type(cleanup_error)) as excinfo:
        await buffer.aclose()
    assert excinfo.value is cleanup_error
    buffer.close()
    await buffer.aclose()
    source.aclose.assert_awaited_once_with()
    source.close.assert_not_called()


def test_failed_async_cleanup_does_not_retry_during_gc(buffer_cls, monkeypatch):
    unraisable = []
    monkeypatch.setattr(sys, "unraisablehook", unraisable.append)
    source = Mock(gen=iter(()), exception_tag=None, aclose=AsyncMock(side_effect=ValueError))

    async def run():
        buffer = buffer_cls(source)
        with pytest.raises(ValueError):
            await buffer.aclose()

    asyncio.run(run())
    gc.collect()
    assert not unraisable
    source.close.assert_not_called()
