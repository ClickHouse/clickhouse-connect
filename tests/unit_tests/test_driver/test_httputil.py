import multiprocessing
import os
import threading
import weakref
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import Mock, patch

import pytest

from clickhouse_connect.driver import httputil
from clickhouse_connect.driver.compression import _zstd_compress
from clickhouse_connect.driver.exceptions import OperationalError
from clickhouse_connect.driver.httputil import ResponseSource

_PROCESS_START_METHODS = [method for method in ("spawn", "fork", "forkserver") if method in multiprocessing.get_all_start_methods()]


def _child_default_manager_report(client_count: int) -> tuple[bool, int, int, int, bool]:
    before = len(httputil.all_managers)
    first = httputil.default_pool_manager()
    same = all(httputil.default_pool_manager() is first for _ in range(client_count))
    return same, before, len(httputil.all_managers), os.getpid(), first in httputil.all_managers


def _child_http_client_manager_count(client_count: int) -> tuple[int, int, bool]:
    from clickhouse_connect.driver.client import Client
    from clickhouse_connect.driver.httpclient import HttpClient

    before = len(httputil.all_managers)
    with patch.object(Client, "_init_common_settings", autospec=True):
        for _ in range(client_count):
            client = HttpClient(
                interface="http",
                host="localhost",
                port=8123,
                username="default",
                password="",
                database="default",
            )
            client.close()
    manager = httputil.default_pool_manager()
    return before, len(httputil.all_managers), manager in httputil.all_managers


def _send_child_default_manager_status(connection) -> None:
    manager = httputil.default_pool_manager()
    connection.send((os.getpid(), id(manager), manager in httputil.all_managers))
    connection.close()


def _send_child_proxy_manager_status(connection, host: str, proxy: str, parent_manager) -> None:
    manager = httputil.get_proxy_manager(host, proxy)
    key = f"{host}__{proxy}"
    connection.send(
        (
            manager is parent_manager,
            httputil._proxy_managers.get(key) is manager,
            manager in httputil.all_managers,
        )
    )
    connection.close()


def _inherited_lock(lock_name: str):
    # Return only the lock. A manager or pool reference here would be inherited by the child and pin the object.
    if lock_name == "manager_pool":
        return httputil.default_pool_manager().pools.lock, None
    if lock_name == "default_manager":
        return httputil._default_pool_lock, None
    if lock_name == "default_connection_queue":
        return httputil.default_pool_manager().connection_from_host("fork-lock.example", 80).pool.mutex, None
    # all_managers is the only owner of this manager.
    manager = httputil.get_pool_manager()
    manager_ref = weakref.ref(manager)
    return manager.connection_from_host("fork-lock.example", 80).pool.mutex, manager_ref


@pytest.fixture
def restore_manager_registries():
    snapshot = dict(httputil.all_managers)
    proxy_snapshot = dict(httputil._proxy_managers)
    yield
    httputil.all_managers.clear()
    httputil.all_managers.update(snapshot)
    httputil._proxy_managers.clear()
    httputil._proxy_managers.update(proxy_snapshot)


@pytest.fixture
def restore_default_pool_manager(restore_manager_registries):
    manager = httputil._default_pool_manager
    pid = httputil._default_pool_pid
    lock = httputil._default_pool_lock
    inherited = list(httputil._inherited_managers)
    yield
    httputil._default_pool_manager = manager
    httputil._default_pool_pid = pid
    httputil._default_pool_lock = lock
    httputil._inherited_managers[:] = inherited


class TestDefaultPoolManager:
    def test_reused_within_process(self):
        first = httputil.default_pool_manager()
        assert httputil.default_pool_manager() is first
        assert first in httputil.all_managers

    def test_pid_change_replaces_inherited_manager_once(self, restore_default_pool_manager):
        parent = httputil.default_pool_manager()

        with patch.object(parent, "clear") as clear:
            httputil._reset_pool_state_after_fork()
            child = httputil.default_pool_manager()

        assert child is not parent
        assert httputil.default_pool_manager() is child
        assert parent not in httputil.all_managers
        assert child in httputil.all_managers
        clear.assert_not_called()

    def test_http_clients_after_pid_change_do_not_leak_managers(self, restore_default_pool_manager):
        from clickhouse_connect.driver.client import Client
        from clickhouse_connect.driver.httpclient import HttpClient

        parent = httputil.default_pool_manager()
        httputil._reset_pool_state_after_fork()
        before = len(httputil.all_managers)

        with patch.object(Client, "_init_common_settings", autospec=True):
            for _ in range(20):
                client = HttpClient(
                    interface="http",
                    host="localhost",
                    port=8123,
                    username="default",
                    password="",
                    database="default",
                )
                client.close()

        assert parent not in httputil.all_managers
        assert len(httputil.all_managers) <= before + 1
        assert httputil.default_pool_manager() in httputil.all_managers

    def test_concurrent_pid_rollover_creates_one_manager(self, monkeypatch, restore_default_pool_manager):
        worker_count = 8
        httputil._default_pool_pid = -1
        manager_barrier = threading.Barrier(worker_count)
        original_get_pool_manager = httputil.get_pool_manager
        created = []

        def tracked_get_pool_manager():
            try:
                manager_barrier.wait(timeout=0.5)
            except threading.BrokenBarrierError:
                pass
            manager = original_get_pool_manager()
            created.append(manager)
            return manager

        monkeypatch.setattr(httputil, "get_pool_manager", tracked_get_pool_manager)
        with ThreadPoolExecutor(max_workers=worker_count) as executor:
            futures = [executor.submit(httputil.default_pool_manager) for _ in range(worker_count)]
            managers = [future.result(timeout=5) for future in futures]

        assert len(created) == 1
        assert all(manager is managers[0] for manager in managers)
        assert managers[0] in httputil.all_managers

    @pytest.mark.parametrize("lock_name", ["manager_pool", "default_manager", "default_connection_queue", "unowned_connection_queue"])
    @pytest.mark.skipif("fork" not in _PROCESS_START_METHODS, reason="fork is unavailable")
    def test_fork_does_not_block_on_inherited_lock(self, lock_name):
        # No registry fixture here. Its snapshot would hold a manager reference that the child inherits.
        inherited_lock, unowned_manager_ref = _inherited_lock(lock_name)
        parent_id = id(httputil._default_pool_manager)
        pool_locked = threading.Event()
        release_pool = threading.Event()

        def hold_pool_lock():
            with inherited_lock:
                pool_locked.set()
                release_pool.wait()

        lock_thread = threading.Thread(target=hold_pool_lock)
        lock_thread.start()
        assert pool_locked.wait(timeout=5)

        ctx = multiprocessing.get_context("fork")
        receiver, sender = ctx.Pipe(duplex=False)
        process = ctx.Process(target=_send_child_default_manager_status, args=(sender,))
        received = False
        status = None
        try:
            process.start()
            sender.close()
            release_pool.set()
            received = receiver.poll(5)
            if received:
                status = receiver.recv()
        finally:
            release_pool.set()
            lock_thread.join(timeout=5)
            if process.is_alive():
                process.terminate()
            process.join(timeout=5)
            receiver.close()
            if unowned_manager_ref is not None:
                unowned_manager = unowned_manager_ref()
                if unowned_manager is not None:
                    unowned_manager.clear()
                    httputil.all_managers.pop(unowned_manager, None)

        assert received, "child blocked while replacing the inherited pool manager"
        assert process.exitcode == 0
        assert status is not None
        child_pid, child_manager_id, registered = status
        assert child_pid == process.pid
        assert child_manager_id != parent_id
        assert registered

    @pytest.mark.skipif("fork" not in _PROCESS_START_METHODS, reason="fork is unavailable")
    def test_fork_does_not_reuse_inherited_proxy_manager(self, restore_manager_registries):
        host = "fork-cache.example"
        proxy = "http://127.0.0.1:1"
        parent_manager = httputil.get_proxy_manager(host, proxy)
        ctx = multiprocessing.get_context("fork")
        receiver, sender = ctx.Pipe(duplex=False)
        process = ctx.Process(
            target=_send_child_proxy_manager_status,
            args=(sender, host, proxy, parent_manager),
        )
        received = False
        status = None
        try:
            process.start()
            sender.close()
            received = receiver.poll(5)
            if received:
                status = receiver.recv()
        finally:
            if process.is_alive():
                process.terminate()
            process.join(timeout=5)
            receiver.close()

        assert received, "child blocked while replacing the inherited proxy manager"
        assert process.exitcode == 0
        assert status == (False, True, True)

    @pytest.mark.parametrize("start_method", _PROCESS_START_METHODS)
    def test_child_process_reuses_one_default_manager(self, start_method):
        ctx = multiprocessing.get_context(start_method)
        with ctx.Pool(1) as pool:
            same, before, after, child_pid, registered = pool.apply(_child_default_manager_report, (25,))
        assert child_pid != os.getpid()
        assert same
        assert after <= before + 1
        assert registered

    @pytest.mark.parametrize("start_method", _PROCESS_START_METHODS)
    def test_child_http_clients_do_not_retain_a_manager_per_close(self, start_method):
        ctx = multiprocessing.get_context(start_method)
        with ctx.Pool(1) as pool:
            before, after, registered = pool.apply(_child_http_client_manager_count, (20,))
        assert after <= before + 1
        assert registered


class TestResponseSourceZstd:
    def test_zstd_response_decompressed_correctly(self):
        original = b"clickhouse row data " * 200
        compressed = _zstd_compress(original)
        chunk_size = len(compressed) // 3
        raw_chunks = [compressed[i : i + chunk_size] for i in range(0, len(compressed), chunk_size)]

        mock_response = Mock()
        mock_response.headers = {"content-encoding": "zstd"}

        def zstd_stream(chunk_size, decompress):
            yield from raw_chunks

        mock_response.stream = zstd_stream
        source = ResponseSource(mock_response, chunk_size=1024 * 1024)

        result = b"".join(source.gen)
        assert result == original


class TestResponseSourceNetworkError:
    """Test ResponseSource handling of network errors"""

    def test_network_error_before_any_data_raises_exception(self):
        """Test that a network error before receiving any data raises OperationalError"""
        mock_response = Mock()
        mock_response.headers = {}

        def failing_stream(chunk_size, decompress):
            """Generator that raises an exception immediately (simulating network failure)"""
            raise ConnectionError("Connection reset by peer")
            yield

        mock_response.stream = failing_stream
        source = ResponseSource(mock_response, chunk_size=1024)

        with pytest.raises(OperationalError) as excinfo:
            list(source.gen)

        assert "Failed to read response data from server" in str(excinfo.value)
        assert isinstance(excinfo.value.__cause__, ConnectionError)

    def test_network_error_after_data_received_does_raise(self):
        """Test that a network error after some data was received raises an exception"""
        mock_response = Mock()
        mock_response.headers = {}

        def partial_stream(chunk_size, decompress):
            """Generator that yields one chunk then fails"""
            yield b"first chunk of data"
            raise ConnectionError("Connection lost")

        mock_response.stream = partial_stream
        source = ResponseSource(mock_response, chunk_size=1024)

        received = []
        with pytest.raises(OperationalError) as excinfo:
            for chunk in source.gen:
                received.append(chunk)

        assert received == [b"first chunk of data"]
        assert "Failed to read response data from server" in str(excinfo.value)
        assert isinstance(excinfo.value.__cause__, ConnectionError)

    def test_normal_empty_response_does_not_raise(self):
        """Test that a legitimately empty response (no error) does not raise an exception"""
        mock_response = Mock()
        mock_response.headers = {}

        def empty_stream(chunk_size, decompress):
            """Generator that returns no data (empty result set)"""
            return
            yield

        mock_response.stream = empty_stream
        source = ResponseSource(mock_response, chunk_size=1024)
        chunks = list(source.gen)

        assert len(chunks) == 0

    def test_network_error_with_compressed_response(self):
        """Test network error handling with compressed (lz4) response"""
        mock_response = Mock()
        mock_response.headers = {"content-encoding": "lz4"}

        def failing_stream(chunk_size, decompress):
            """Generator that raises an exception immediately"""
            raise ConnectionError("Network error during compressed transfer")
            yield

        mock_response.stream = failing_stream
        source = ResponseSource(mock_response, chunk_size=1024)

        with pytest.raises(OperationalError) as excinfo:
            list(source.gen)

        assert "Failed to read response data from server" in str(excinfo.value)
