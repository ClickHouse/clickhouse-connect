import asyncio
import threading
import uuid

import pytest

from clickhouse_connect import create_client, get_async_client
from clickhouse_connect.driver.exceptions import ProgrammingError
from tests.integration_tests.conftest import TestConfig, make_client_config


def test_sync_client_sequential_thread_access(test_client, test_config: TestConfig):
    """Test that sync clients can handle sequential access from different threads."""
    if test_config.cloud:
        pytest.skip("Skipping threading test in ClickHouse Cloud")

    results = []
    errors = []

    def run_query(value):
        try:
            result = test_client.command(f"SELECT {value}")
            results.append(result)
        except Exception as ex:  # pylint: disable=broad-exception-caught
            errors.append(ex)

    threads = [threading.Thread(target=run_query, args=(i,)) for i in range(3)]
    for thread in threads:
        thread.start()
        thread.join()

    assert len(errors) == 0, f"Unexpected errors: {errors}"
    assert len(results) == 3
    assert results == [0, 1, 2]


@pytest.mark.asyncio
async def test_async_client_threadsafe_submission(test_native_async_client, test_config: TestConfig):
    """Test that async clients work correctly with run_coroutine_threadsafe from multiple threads."""
    if test_config.cloud:
        pytest.skip("Skipping threading test in ClickHouse Cloud")

    loop = asyncio.get_running_loop()
    results = []
    errors = []
    lock = threading.Lock()

    def run_query_threadsafe(value):
        try:
            future = asyncio.run_coroutine_threadsafe(
                test_native_async_client.command(f"SELECT {value}"),
                loop
            )
            result = future.result(timeout=5)
            with lock:
                results.append(result)
        except Exception as ex:  # pylint: disable=broad-exception-caught
            with lock:
                errors.append(ex)

    threads = [threading.Thread(target=run_query_threadsafe, args=(i,)) for i in range(3)]
    for thread in threads:
        thread.start()

    await asyncio.sleep(2)

    for thread in threads:
        thread.join()

    assert len(errors) == 0, f"Unexpected errors: {errors}"
    assert len(results) == 3
    assert sorted(results) == [0, 1, 2]


def test_sync_concurrent_session_usage_detection(test_config: TestConfig):
    """Test that ClickHouse server detects concurrent usage of the same session (sync client)."""
    if test_config.cloud:
        pytest.skip("Skipping session concurrency test in ClickHouse Cloud")

    session_id = str(uuid.uuid4())
    client1 = create_client(**make_client_config(test_config, session_id=session_id))
    client2 = create_client(**make_client_config(test_config, session_id=session_id))

    thrown = []

    def run_query(client):
        try:
            client.command("SELECT sleep(1)")
        except (ProgrammingError, Exception) as ex:  # pylint: disable=broad-exception-caught
            thrown.append(ex)

    threads = [
        threading.Thread(target=run_query, args=(client1,)),
        threading.Thread(target=run_query, args=(client2,))
    ]

    for thread in threads:
        thread.start()

    for thread in threads:
        thread.join()

    try:
        client1.close()
        client2.close()
    except Exception:  # pylint: disable=broad-exception-caught
        pass

    # At least one should fail due to concurrent session usage
    assert len(thrown) > 0, "Expected ClickHouse to detect concurrent session usage"
    assert any("concurrent" in str(ex).lower() or "session" in str(ex).lower() for ex in thrown), \
        f"Expected session concurrency error, got: {thrown}"


@pytest.mark.asyncio
async def test_async_concurrent_session_usage_detection(test_config: TestConfig):
    """Test that ClickHouse server detects concurrent usage of the same session (async client)."""
    if test_config.cloud:
        pytest.skip("Skipping session concurrency test in ClickHouse Cloud")

    session_id = str(uuid.uuid4())
    loop = asyncio.get_running_loop()

    async with await get_async_client(**make_client_config(test_config, session_id=session_id)) as client1, \
               await get_async_client(**make_client_config(test_config, session_id=session_id)) as client2:

        thrown = []

        def run_query(client):
            try:
                future = asyncio.run_coroutine_threadsafe(
                    client.command("SELECT sleep(1)"),
                    loop
                )
                future.result(timeout=5)
            except (ProgrammingError, Exception) as ex:  # pylint: disable=broad-exception-caught
                thrown.append(ex)

        threads = [
            threading.Thread(target=run_query, args=(client1,)),
            threading.Thread(target=run_query, args=(client2,))
        ]

        for thread in threads:
            thread.start()

        await asyncio.sleep(2)

        for thread in threads:
            thread.join()

        # At least one should fail due to concurrent session usage
        assert len(thrown) > 0, "Expected ClickHouse to detect concurrent session usage"
        assert any("concurrent" in str(ex).lower() or "session" in str(ex).lower() for ex in thrown), \
            f"Expected session concurrency error, got: {thrown}"
