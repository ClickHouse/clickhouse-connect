import asyncio
import threading
import uuid

import pytest

from clickhouse_connect.driver.exceptions import ProgrammingError
from tests.integration_tests.conftest import TestConfig


def test_sync_client_sequential_thread_access(param_client, client_mode, call, test_config: TestConfig):
    """Test that sync clients can handle sequential access from different threads."""
    if client_mode != "sync":
        pytest.skip("Only testing sync client behavior")

    if test_config.cloud:
        pytest.skip("Skipping threading test in ClickHouse Cloud")

    results = []
    errors = []

    def run_query(value):
        try:
            result = param_client.command(f"SELECT {value}")
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


def test_async_client_threadsafe_submission(param_client, client_mode, call, test_config: TestConfig, shared_loop):
    """Test that async clients work correctly with run_coroutine_threadsafe from multiple threads."""
    if client_mode != "async":
        pytest.skip("Only testing async client behavior")

    if test_config.cloud:
        pytest.skip("Skipping threading test in ClickHouse Cloud")

    results = []
    errors = []
    lock = threading.Lock()

    def run_query_threadsafe(value):
        try:
            future = asyncio.run_coroutine_threadsafe(
                param_client.command(f"SELECT {value}"),
                shared_loop
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

    call(asyncio.sleep, 2)

    for thread in threads:
        thread.join()

    assert len(errors) == 0, f"Unexpected errors: {errors}"
    assert len(results) == 3
    assert sorted(results) == [0, 1, 2]


def test_concurrent_session_usage_detection(client_mode, call, test_config: TestConfig, client_factory, shared_loop):
    """Test that ClickHouse server detects concurrent usage of the same session."""
    if test_config.cloud:
        pytest.skip("Skipping session concurrency test in ClickHouse Cloud")

    session_id = str(uuid.uuid4())
    client1 = client_factory(session_id=session_id)
    client2 = client_factory(session_id=session_id)

    thrown = []

    def run_query(client):
        try:
            if client_mode == "sync":
                client.command("SELECT sleep(1)")
            else:
                future = asyncio.run_coroutine_threadsafe(
                    client.command("SELECT sleep(1)"),
                    shared_loop
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

    if client_mode == "async":
        call(asyncio.sleep, 2)

    for thread in threads:
        thread.join()

    # At least one should fail due to concurrent session usage
    assert len(thrown) > 0, "Expected ClickHouse to detect concurrent session usage"
    assert any("concurrent" in str(ex).lower() or "session" in str(ex).lower() for ex in thrown), \
        f"Expected session concurrency error, got: {thrown}"
