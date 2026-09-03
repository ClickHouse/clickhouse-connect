import multiprocessing
import os
from unittest.mock import Mock, patch

import pytest

from clickhouse_connect.driver import httputil
from clickhouse_connect.driver.compression import _zstd_compress
from clickhouse_connect.driver.exceptions import OperationalError
from clickhouse_connect.driver.httputil import ResponseSource


def _child_default_manager_report(client_count: int) -> tuple[bool, int, int]:
    first = httputil.default_pool_manager()
    same = all(httputil.default_pool_manager() is first for _ in range(client_count))
    return same, len(httputil.all_managers), os.getpid()


def _child_http_client_manager_count(client_count: int) -> int:
    from clickhouse_connect.driver.client import Client
    from clickhouse_connect.driver.httpclient import HttpClient

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
    return len(httputil.all_managers)


@pytest.fixture
def restore_default_pool_manager():
    manager = httputil._default_pool_manager
    pid = httputil._default_pool_pid
    snapshot = dict(httputil.all_managers)
    yield
    httputil._default_pool_manager = manager
    httputil._default_pool_pid = pid
    httputil.all_managers.clear()
    httputil.all_managers.update(snapshot)


class TestDefaultPoolManager:
    def test_reused_within_process(self):
        first = httputil.default_pool_manager()
        assert httputil.default_pool_manager() is first
        assert first in httputil.all_managers

    def test_pid_change_replaces_inherited_manager_once(self, monkeypatch, restore_default_pool_manager):
        parent = httputil.default_pool_manager()
        parent_pid = httputil._default_pool_pid
        monkeypatch.setattr(httputil.os, "getpid", lambda: parent_pid + 1)

        child = httputil.default_pool_manager()
        assert child is not parent
        assert httputil.default_pool_manager() is child
        assert parent not in httputil.all_managers
        assert child in httputil.all_managers

    def test_http_clients_after_pid_change_do_not_leak_managers(self, monkeypatch, restore_default_pool_manager):
        from clickhouse_connect.driver.client import Client
        from clickhouse_connect.driver.httpclient import HttpClient

        parent = httputil.default_pool_manager()
        before = len(httputil.all_managers)
        monkeypatch.setattr(httputil.os, "getpid", lambda: httputil._default_pool_pid + 1)

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
        assert len(httputil.all_managers) <= before
        assert httputil.default_pool_manager() in httputil.all_managers

    @pytest.mark.parametrize("start_method", [method for method in ("spawn", "fork") if method in multiprocessing.get_all_start_methods()])
    def test_child_process_reuses_one_default_manager(self, start_method):
        ctx = multiprocessing.get_context(start_method)
        with ctx.Pool(1) as pool:
            same, remaining, child_pid = pool.apply(_child_default_manager_report, (25,))
        assert child_pid != os.getpid()
        assert same
        assert remaining == 1

    @pytest.mark.parametrize("start_method", [method for method in ("spawn", "fork") if method in multiprocessing.get_all_start_methods()])
    def test_child_http_clients_do_not_retain_a_manager_per_close(self, start_method):
        ctx = multiprocessing.get_context(start_method)
        with ctx.Pool(1) as pool:
            remaining = pool.apply(_child_http_client_manager_count, (20,))
        assert remaining == 1


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
