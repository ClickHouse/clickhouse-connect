"""Real-core buffer ownership through the driver's streaming result boundary."""

import asyncio
import gc
import queue
import threading
import weakref
from concurrent.futures import ThreadPoolExecutor

import pytest

from clickhouse_connect.datatypes.registry import get_from_name
from clickhouse_connect.driver import options, rustcodec
from clickhouse_connect.driver.exceptions import DataError
from clickhouse_connect.driver.query import QueryContext
from clickhouse_connect.driver.rustcodec import _RustNativeTransform
from tests.helpers import native_insert_block

pytest.importorskip("_ch_core")
np = pytest.importorskip("numpy")


class _Source:
    exception_tag = None

    def __init__(self, chunk, count=100):
        self.closed = threading.Event()
        self.blocked = threading.Event()
        self.reads = 0
        self.block_after = 0
        self.gen = self.chunks(chunk, count)

    def chunks(self, chunk, count):
        for _ in range(count):
            if self.closed.is_set():
                return
            self.reads += 1
            if self.reads == self.block_after:
                self.blocked.set()
            yield chunk

    def close(self):
        self.closed.set()


def _result(source):
    context = QueryContext(use_numpy=True, streaming=True)
    context.block_info = False
    return _RustNativeTransform(strict=True).parse_response(source, context)


def _chunk():
    return native_insert_block([[13], [79], [113]], ["n"], [get_from_name("UInt64")])


@pytest.mark.parametrize("release", ["close", "abandon", "error"])
def test_buffer_views_survive_worker_conversion_and_release(monkeypatch, release):
    source = _Source(_chunk())
    executor = ThreadPoolExecutor(max_workers=1)
    owner = None
    try:
        result = executor.submit(_result, source).result(timeout=5)
        read_source = result.source
        owner = weakref.ref(read_source)
        source.block_after = read_source.queue.maxsize + 3
        columns = executor.submit(next, result._block_gen).result(timeout=5)
        view = columns[0][1:]
        assert not view.flags.writeable
        second = executor.submit(next, result._block_gen).result(timeout=5)
        assert source.blocked.wait(5)
        assert read_source.queue.full()
        assert source.reads == source.block_after
        thread = read_source._thread
        assert thread is not None and thread.is_alive()
        if release == "error":

            def fail(*_args):
                raise ValueError("conversion failed")

            monkeypatch.setattr("clickhouse_connect.driver.rustcodec._convert_block", fail)
            with pytest.raises(DataError, match="conversion failed"):
                executor.submit(next, result._block_gen).result(timeout=5)
        elif release == "close":
            executor.submit(result.close).result(timeout=5)
        del columns, second, result, read_source
        gc.collect()
        thread.join(5)
        assert not thread.is_alive()
        assert source.closed.is_set()
        assert owner() is None
        np.testing.assert_array_equal(view, [79, 113])
        holder = [view]
        del view
        executor.submit(holder.clear).result(timeout=5)
    finally:
        source.close()
        remaining = owner() if owner else None
        if remaining is not None:
            remaining.close()
            remaining.queue.put_nowait(("eof", None))
        executor.shutdown(wait=True, cancel_futures=True)


@pytest.mark.asyncio
@pytest.mark.parametrize("as_pandas", [False, True], ids=["numpy", "pandas"])
async def test_pending_numpy_stream_cancellation_releases_worker(monkeypatch, as_pandas):
    if as_pandas:
        pytest.importorskip("pandas")
        options.check_pandas()
    entered_get = threading.Event()
    consumer_done = threading.Event()
    release_source = threading.Event()

    class TrackedQueue(queue.Queue):
        def get(self, block=True, timeout=None):
            if block:
                entered_get.set()
            return super().get(block, timeout)

    class BlockedSource(_Source):
        def chunks(self, chunk, count):
            yield chunk
            yield chunk
            release_source.wait(5)

        def close(self):
            super().close()
            release_source.set()

    class ObservedReadAheadSource(rustcodec.ReadAheadSource):
        def __init__(self, source):
            super().__init__(source, maxsize=2)
            self.queue = TrackedQueue(maxsize=2)

    monkeypatch.setattr(rustcodec, "ReadAheadSource", ObservedReadAheadSource)
    source = BlockedSource(_chunk())
    context = QueryContext(use_numpy=True, as_pandas=as_pandas, streaming=True)
    result = _RustNativeTransform(strict=True).parse_response(source, context)
    read_source = result.source
    stream = result.df_stream if as_pandas else result.np_stream
    original = stream.gen

    def observed():
        try:
            yield from original
        finally:
            consumer_done.set()

    stream.gen = observed()
    task = None
    try:
        async with stream:
            retained = await stream.__anext__()
            await stream.__anext__()
            task = asyncio.create_task(stream.__anext__())
            assert await asyncio.to_thread(entered_get.wait, 2)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
        assert await asyncio.to_thread(consumer_done.wait, 2)
        assert source.closed.is_set()
        assert read_source.source is None
        await asyncio.to_thread(read_source._thread.join, 2)
        assert not read_source._thread.is_alive()
        values = retained["n"].to_numpy() if as_pandas else retained[:, 0]
        np.testing.assert_array_equal(values, [13, 79, 113])
    finally:
        release_source.set()
        read_source.close()
        read_source.queue.put_nowait(("eof", None))
        if task is not None:
            task.cancel()
        await asyncio.to_thread(consumer_done.wait, 2)
