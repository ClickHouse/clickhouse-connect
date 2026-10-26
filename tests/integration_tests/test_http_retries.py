import socket
import sys
import time
from http.client import HTTPConnection, HTTPSConnection
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Thread
from urllib.parse import parse_qs, urlsplit

import pytest

from clickhouse_connect.dbapi.cursor import Cursor
from clickhouse_connect.driver.exceptions import DatabaseError, OperationalError
from clickhouse_connect.driver.options import pd

_HOP_HEADERS = {"connection", "content-length", "expect", "host", "keep-alive", "trailer", "transfer-encoding"}
_REMOTE_CLOSE_FAULTS = ["disconnect", "progress_headers", "status_only"]
_FAULTS = [*_REMOTE_CLOSE_FAULTS, 429, 503, 504]


def _dedup_settings(client, enabled: bool) -> dict[str, int | str]:
    settings: dict[str, int | str] = {"insert_deduplicate": int(enabled)}
    if "deduplicate_insert" in client.server_settings:
        settings["deduplicate_insert"] = "enable" if enabled else "disable"
    return settings


class _RetryHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def do_POST(self):  # noqa: N802
        if self.headers.get("Transfer-Encoding", "").lower() == "chunked":
            chunks = []
            while True:
                size = int(self.rfile.readline().split(b";", 1)[0], 16)
                if not size:
                    while self.rfile.readline() != b"\r\n":
                        pass
                    break
                chunks.append(self.rfile.read(size))
                assert self.rfile.read(2) == b"\r\n"
            body = b"".join(chunks)
        else:
            body = self.rfile.read(int(self.headers.get("Content-Length", 0)))

        upstream = self.server.upstream
        connection_cls = HTTPSConnection if upstream.scheme == "https" else HTTPConnection
        headers = {name: value for name, value in self.headers.items() if name.lower() not in _HOP_HEADERS}
        connection = connection_cls(upstream.hostname, upstream.port, timeout=15)
        try:
            connection.request("POST", self.path, body=body, headers=headers)
            response = connection.getresponse()
            response_body = response.read()
            response_headers = response.getheaders()
            self.server.statuses.append(response.status)
        finally:
            connection.close()

        fault, self.server.fault = self.server.fault, None
        if fault is not None:
            # The body can arrive before ClickHouse releases the query ID.
            self.server.wait_for_query(self.path)
        if fault == "disconnect":
            self.close_connection = True
            self.connection.shutdown(socket.SHUT_RDWR)
            return
        if fault in ("progress_headers", "status_only"):
            self.wfile.write(b"HTTP/1.1 200 OK\r\n")
            if fault == "progress_headers":
                self.wfile.write(b'X-ClickHouse-Progress: {"read_rows":"13"}\r\n')
            self.wfile.flush()
            self.close_connection = True
            self.connection.shutdown(socket.SHUT_WR)
            return
        if fault is not None:
            self.send_response(fault)
            response_body = b"Injected HTTP response failure"
        else:
            self.send_response(response.status)
            for name, value in response_headers:
                if name.lower() not in _HOP_HEADERS:
                    self.send_header(name, value)
        self.send_header("Content-Length", str(len(response_body)))
        self.send_header("Connection", "close")
        self.close_connection = True
        self.end_headers()
        self.wfile.write(response_body)

    def log_message(self, *args):
        pass


class _RetryProxy(ThreadingHTTPServer):
    def __init__(self, upstream_url, observer):
        super().__init__(("127.0.0.1", 0), _RetryHandler)
        self.upstream = urlsplit(upstream_url)
        self.observer = observer
        self.fault = None
        self.statuses = []
        self.errors = []

    def wait_for_query(self, request_path):
        query_id = parse_qs(urlsplit(request_path).query)["query_id"][0]
        deadline = time.monotonic() + 10
        while self.observer.command(
            "SELECT count() FROM system.processes WHERE query_id = {query_id:String}", parameters={"query_id": query_id}
        ):
            if time.monotonic() >= deadline:
                raise AssertionError(f"Query {query_id} is still in system.processes after 10 seconds")
            time.sleep(0.01)

    def handle_error(self, request, client_address):
        self.errors.append(sys.exc_info()[1])
        super().handle_error(request, client_address)

    def arm(self, fault):
        self.statuses.clear()
        self.fault = fault


@pytest.fixture
def retry_proxy(param_client, client_factory, test_create_client):
    observer = test_create_client(autogenerate_session_id=False, connect_timeout=5, send_receive_timeout=5)
    proxy = _RetryProxy(param_client.url, observer)
    thread = Thread(target=proxy.serve_forever, kwargs={"poll_interval": 0.01}, daemon=True)
    thread.start()
    try:
        client = client_factory(host="127.0.0.1", port=proxy.server_port, secure=False)
        yield proxy, client
    finally:
        proxy.shutdown()
        proxy.server_close()
        thread.join(timeout=5)
        observer.close()
        assert not thread.is_alive()
        assert not proxy.errors, proxy.errors


@pytest.mark.parametrize("fault", _FAULTS)
@pytest.mark.parametrize("method", ["command", "query", "raw_query", "raw_stream"])
def test_sql_write_is_not_replayed_after_delivery(retry_proxy, param_client, call, consume_stream, table_context, method, fault):
    proxy, client = retry_proxy
    with table_context("sql_write_retry", ["key Int32"]):
        proxy.arm(fault)
        error = None
        try:
            statement = "INSERT INTO sql_write_retry VALUES (13)" if method == "query" else "INSERT INTO sql_write_retry SELECT 13"
            result = call(getattr(client, method), statement, settings=_dedup_settings(client, False))
            if method == "raw_stream":
                consume_stream(result)
        except DatabaseError as ex:
            error = ex

        rows = call(param_client.query, "SELECT key FROM sql_write_retry").result_rows
        assert rows == [(13,)]
        assert proxy.statuses == [200]
        assert isinstance(error, OperationalError if fault in _REMOTE_CLOSE_FAULTS else DatabaseError)


@pytest.mark.parametrize("fault", _FAULTS)
@pytest.mark.parametrize("client_mode", ["sync"], indirect=True)
def test_dbapi_write_is_not_replayed_after_delivery(retry_proxy, param_client, call, table_context, fault):
    proxy, client = retry_proxy
    with table_context("dbapi_write_retry", ["key Int32"]):
        proxy.arm(fault)
        error = None
        try:
            Cursor(client).execute("INSERT INTO dbapi_write_retry SELECT 13 SETTINGS insert_deduplicate = 0")
        except DatabaseError as ex:
            error = ex

        assert call(param_client.query, "SELECT key FROM dbapi_write_retry").result_rows == [(13,)]
        assert proxy.statuses == [200]
        assert isinstance(error, OperationalError if fault in _REMOTE_CLOSE_FAULTS else DatabaseError)


@pytest.mark.parametrize("fault", _FAULTS)
@pytest.mark.parametrize("method", ["command", "query", "raw_query", "raw_stream"])
def test_read_retries_after_delivery(retry_proxy, call, consume_stream, method, fault):
    proxy, client = retry_proxy
    # Commands retain their remote-close retry but have no HTTP status retry budget.
    proxy.arm(fault)
    if method == "command" and fault not in _REMOTE_CLOSE_FAULTS:
        with pytest.raises(DatabaseError):
            call(client.command, "SELECT 13")
        assert proxy.statuses == [200]
        return
    result = call(getattr(client, method), "SELECT 13")
    if method == "query":
        assert result.result_rows == [(13,)]
    elif method == "raw_stream":
        chunks = []
        consume_stream(result, chunks.append)
        assert b"".join(chunks).strip() == b"13"
    elif method == "command":
        assert result == 13
    else:
        assert result.strip() == b"13"
    assert proxy.statuses == [200, 200]


@pytest.mark.parametrize("method", ["insert", "insert_df", "raw_insert", "raw_insert_preframed"])
@pytest.mark.parametrize("deduplicate", [False, True], ids=["at_least_once", "dedup_window"])
@pytest.mark.parametrize("fault", _REMOTE_CLOSE_FAULTS)
def test_insert_data_retries_after_delivery(retry_proxy, param_client, call, table_context, method, deduplicate, fault):
    if method == "insert_df" and pd is None:
        pytest.skip("pandas not available")
    proxy, client = retry_proxy
    with table_context("insert_data_retry", ["key Int32"], settings={"non_replicated_deduplication_window": 100}):
        proxy.arm(fault)
        settings = _dedup_settings(client, deduplicate)
        if method == "raw_insert_preframed":
            call(client.raw_insert, insert_block=b"INSERT INTO insert_data_retry FORMAT CSV\n13\n79\n", settings=settings)
        elif method == "raw_insert":
            call(client.raw_insert, "insert_data_retry", ["key"], b"13\n79\n", fmt="CSV", settings=settings)
        else:
            data = pd.DataFrame({"key": [13, 79]}) if method == "insert_df" else [[13], [79]]
            call(getattr(client, method), "insert_data_retry", data, column_names=["key"], column_type_names=["Int32"], settings=settings)
        rows = call(param_client.query, "SELECT key FROM insert_data_retry ORDER BY key").result_rows
        expected_rows = [(13,), (79,)] if deduplicate else [(13,), (13,), (79,), (79,)]
        assert rows == expected_rows
        assert proxy.statuses == [200, 200]
