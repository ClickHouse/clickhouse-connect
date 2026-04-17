import asyncio
import threading
import time

import pytest

from clickhouse_connect.driver.asyncqueue import EOF_SENTINEL, AsyncSyncQueue, Empty


def test_async_put_sync_get():
    """Test async producer putting items, sync consumer getting them."""
    queue = AsyncSyncQueue(maxsize=5)
    items_received = []

    async def async_producer():
        """Put items from async context."""
        for i in range(10):
            await queue.async_q.put(f"item_{i}")
        await queue.async_q.put(EOF_SENTINEL)

    def sync_consumer():
        """Get items from sync context."""
        while True:
            item = queue.sync_q.get()
            if item is EOF_SENTINEL:
                break
            items_received.append(item)

    async def run_test():
        consumer_thread = threading.Thread(target=sync_consumer)
        consumer_thread.start()

        await async_producer()

        consumer_thread.join(timeout=5.0)
        assert not consumer_thread.is_alive(), "Consumer thread hung"

    asyncio.run(run_test())

    assert len(items_received) == 10
    assert items_received == [f"item_{i}" for i in range(10)]


def test_sync_put_async_get():
    """Test sync producer putting items, async consumer getting them."""
    queue = AsyncSyncQueue(maxsize=5)
    items_received = []

    def sync_producer():
        """Put items from sync context."""
        for i in range(10):
            queue.sync_q.put(f"item_{i}")
        queue.sync_q.put(EOF_SENTINEL)

    async def async_consumer():
        """Get items from async context."""
        while True:
            item = await queue.async_q.get()
            if item is EOF_SENTINEL:
                break
            items_received.append(item)

    async def run_test():
        producer_thread = threading.Thread(target=sync_producer)
        producer_thread.start()

        await async_consumer()

        producer_thread.join(timeout=5.0)
        assert not producer_thread.is_alive(), "Producer thread hung"

    asyncio.run(run_test())

    assert len(items_received) == 10
    assert items_received == [f"item_{i}" for i in range(10)]


def test_backpressure_async_producer():
    """Test that bounded queue provides backpressure to async producer."""
    queue = AsyncSyncQueue(maxsize=3)
    produced = []
    consumed = []

    async def fast_producer():
        """Producer that tries to produce faster than consumer."""
        for i in range(10):
            produced.append(f"before_put_{i}")
            await queue.async_q.put(f"item_{i}")
            produced.append(f"after_put_{i}")
        await queue.async_q.put(EOF_SENTINEL)

    def slow_consumer():
        """Consumer that's slower than producer."""
        while True:
            time.sleep(0.01)
            item = queue.sync_q.get()
            if item is EOF_SENTINEL:
                break
            consumed.append(item)

    async def run_test():
        consumer_thread = threading.Thread(target=slow_consumer)
        consumer_thread.start()

        await fast_producer()

        consumer_thread.join(timeout=5.0)
        assert not consumer_thread.is_alive()

    asyncio.run(run_test())

    assert len(consumed) == 10
    assert consumed == [f"item_{i}" for i in range(10)]


def test_backpressure_sync_producer():
    """Test that bounded queue provides backpressure to sync producer."""
    queue = AsyncSyncQueue(maxsize=3)
    produced = []
    consumed = []

    def fast_producer():
        """Producer that tries to produce faster than consumer."""
        for i in range(10):
            produced.append(f"before_put_{i}")
            queue.sync_q.put(f"item_{i}")
            produced.append(f"after_put_{i}")
        queue.sync_q.put(EOF_SENTINEL)

    async def slow_consumer():
        """Consumer that's slower than producer."""
        while True:
            await asyncio.sleep(0.01)
            item = await queue.async_q.get()
            if item is EOF_SENTINEL:
                break
            consumed.append(item)

    async def run_test():
        producer_thread = threading.Thread(target=fast_producer)
        producer_thread.start()

        await slow_consumer()

        producer_thread.join(timeout=5.0)
        assert not producer_thread.is_alive()

    asyncio.run(run_test())

    assert len(consumed) == 10
    assert consumed == [f"item_{i}" for i in range(10)]


def test_shutdown_unblocks_consumer():
    """Test that shutdown() unblocks a consumer waiting on an empty queue."""
    queue = AsyncSyncQueue(maxsize=2)
    consumer_unblocked = threading.Event()

    def blocking_consumer():
        """Consumer that will block waiting for items."""
        try:
            item = queue.sync_q.get(timeout=2.0)
            if item is EOF_SENTINEL:
                consumer_unblocked.set()
        except Exception:
            pass

    async def run_test():
        consumer_thread = threading.Thread(target=blocking_consumer)
        consumer_thread.start()

        await asyncio.sleep(0.1)

        queue.shutdown()

        consumer_thread.join(timeout=2.0)
        assert consumer_unblocked.is_set(), "Consumer was not unblocked by shutdown"

    asyncio.run(run_test())


def test_shutdown_unblocks_producer():
    """Test that shutdown() unblocks a producer waiting on a full queue."""
    queue = AsyncSyncQueue(maxsize=2)
    producer_unblocked = threading.Event()

    async def blocking_producer():
        """Producer that will block when queue is full."""
        try:
            await queue.async_q.put("item1")
            await queue.async_q.put("item2")

            await asyncio.wait_for(queue.async_q.put("item3"), timeout=2.0)
        except (RuntimeError, asyncio.TimeoutError):
            producer_unblocked.set()
        except Exception as e:
            print(f"Producer caught unexpected exception: {e}")

    async def run_test():
        producer_task = asyncio.create_task(blocking_producer())

        await asyncio.sleep(0.1)

        queue.shutdown()

        await producer_task
        assert producer_unblocked.is_set(), "Producer was not unblocked by shutdown"

    asyncio.run(run_test())


def test_multiple_producers_single_consumer():
    """Test multiple async producers with single sync consumer."""
    queue = AsyncSyncQueue(maxsize=10)
    items_received = []

    async def producer(producer_id, count):
        """Producer that sends count items."""
        for i in range(count):
            await queue.async_q.put(f"p{producer_id}_item{i}")

    def consumer():
        """Consumer that reads until getting 30 items (3 producers × 10 items)."""
        received = 0
        while received < 30:
            item = queue.sync_q.get(timeout=5.0)
            items_received.append(item)
            received += 1

    async def run_test():
        consumer_thread = threading.Thread(target=consumer)
        consumer_thread.start()

        await asyncio.gather(producer(0, 10), producer(1, 10), producer(2, 10))

        consumer_thread.join(timeout=5.0)
        assert not consumer_thread.is_alive()

    asyncio.run(run_test())

    assert len(items_received) == 30
    assert len(set(items_received)) == 30


def test_exception_propagation():
    """Test that exceptions can be passed through the queue."""
    queue = AsyncSyncQueue(maxsize=5)
    exception_received = []

    async def producer_with_error():
        """Producer that sends an exception."""
        await queue.async_q.put("item1")
        await queue.async_q.put("item2")
        await queue.async_q.put(ValueError("test error"))
        await queue.async_q.put(EOF_SENTINEL)

    def consumer():
        """Consumer that should receive the exception."""
        items = []
        while True:
            item = queue.sync_q.get()
            if item is EOF_SENTINEL:
                break
            if isinstance(item, Exception):
                exception_received.append(item)
            else:
                items.append(item)
        return items

    async def run_test():
        consumer_thread = threading.Thread(target=consumer)
        consumer_thread.start()

        await producer_with_error()

        consumer_thread.join(timeout=5.0)
        assert not consumer_thread.is_alive()

    asyncio.run(run_test())

    assert len(exception_received) == 1
    assert isinstance(exception_received[0], ValueError)
    assert str(exception_received[0]) == "test error"


def test_empty_exception_on_non_blocking_get():
    """Test that non-blocking get raises Empty when queue is empty."""
    queue = AsyncSyncQueue(maxsize=5)

    with pytest.raises(Empty):
        queue.sync_q.get(block=False)


def test_shutdown_then_cancel_no_invalid_state():
    """Regression: shutdown() + task.cancel() on a blocked putter must not
    raise InvalidStateError.

    Before the fix, shutdown() scheduled fut.set_result via call_soon_threadsafe
    after a not-fut.done() check. task.cancel() could cancel the future before
    the callback ran, so set_result hit an already-cancelled future.
    """
    errors = []

    async def run_test():
        async def wait_for_putter_registration(q: AsyncSyncQueue, timeout: float = 1.0):
            async def _wait():
                while len(q._async_putters) != 1:
                    await asyncio.sleep(0)

            await asyncio.wait_for(_wait(), timeout=timeout)

        loop = asyncio.get_running_loop()
        old_handler = loop.get_exception_handler()

        def exception_handler(_loop, context):
            exc = context.get("exception")
            if exc:
                errors.append(exc)

        loop.set_exception_handler(exception_handler)
        try:
            q = AsyncSyncQueue(maxsize=1)
            producer_blocked = asyncio.Event()

            async def producer():
                await q.async_q.put(b"chunk_1")
                producer_blocked.set()
                await q.async_q.put(b"chunk_2")  # blocks, queue full

            task = loop.create_task(producer())
            await producer_blocked.wait()

            await wait_for_putter_registration(q)
            assert len(q._async_putters) == 1, "Producer never registered its putter future"

            q.shutdown()
            task.cancel()

            try:
                await task
            except asyncio.CancelledError:
                pass

            await asyncio.sleep(0)

        finally:
            loop.set_exception_handler(old_handler)

    asyncio.run(run_test())

    invalid_state_errors = [e for e in errors if isinstance(e, asyncio.InvalidStateError)]
    assert not invalid_state_errors, (
        f"Got {len(invalid_state_errors)} InvalidStateError(s). _safe_set_result should guard against set_result on cancelled futures."
    )


def test_shutdown_still_wakes_async_getter():
    """After the _safe_set_result fix, shutdown() must still wake async getters
    so they see EOF_SENTINEL instead of hanging."""

    async def run_test():
        q = AsyncSyncQueue(maxsize=10)
        got_sentinel = asyncio.Event()

        async def consumer():
            result = await q.async_q.get()
            if result is EOF_SENTINEL:
                got_sentinel.set()

        loop = asyncio.get_running_loop()
        task = loop.create_task(consumer())

        for _ in range(200):
            if len(q._async_getters) == 1:
                break
            await asyncio.sleep(0.001)
        assert len(q._async_getters) == 1, "Consumer never registered its getter future"

        q.shutdown()
        await asyncio.wait_for(task, timeout=2.0)
        assert got_sentinel.is_set(), "Consumer should have received EOF_SENTINEL from shutdown"

    asyncio.run(run_test())


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
