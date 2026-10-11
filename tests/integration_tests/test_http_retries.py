import socket
from http.client import HTTPConnection, HTTPSConnection
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Thread
from urllib.parse import urlsplit

import pytest

from clickhouse_connect.dbapi.cursor import Cursor
from clickhouse_connect.driver.exceptions import DatabaseError, OperationalError
from clickhouse_connect.driver.options import pd

_HOP_HEADERS = {"connection", "content-length", "expect", "host", "keep-alive", "trailer", "transfer-encoding"}
_FAULTS = [pytest.param("disconnect", id="disconnect"), 429, 503, 504]


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

        # The first request has finished on ClickHouse before the client sees the fault.
        fault, self.server.fault = self.server.fault, None
        if fault == "disconnect":
            self.close_connection = True
            self.connection.shutdown(socket.SHUT_RDWR)
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
    def __init__(self, upstream_url):
        super().__init__(("127.0.0.1", 0), _RetryHandler)
        self.upstream = urlsplit(upstream_url)
        self.fault = None
        self.statuses = []

    def arm(self, fault):
        self.statuses.clear()
        self.fault = fault


@pytest.fixture
def retry_proxy(param_client, client_factory):
    proxy = _RetryProxy(param_client.url)
    thread = Thread(target=proxy.serve_forever, kwargs={"poll_interval": 0.01}, daemon=True)
    thread.start()
    try:
        client = client_factory(host="127.0.0.1", port=proxy.server_port, secure=False)
        yield proxy, client
    finally:
        proxy.shutdown()
        proxy.server_close()
        thread.join(timeout=5)
        assert not thread.is_alive()


@pytest.mark.parametrize("fault", _FAULTS)
@pytest.mark.parametrize("method", ["command", "query", "raw_query", "raw_stream"])
def test_sql_write_is_not_replayed_after_delivery(retry_proxy, param_client, call, consume_stream, table_context, method, fault):
    proxy, client = retry_proxy
    with table_context("sql_write_retry", ["key Int32"]):
        proxy.arm(fault)
        error = None
        try:
            statement = "INSERT INTO sql_write_retry VALUES (13)" if method == "query" else "INSERT INTO sql_write_retry SELECT 13"
            result = call(getattr(client, method), statement, settings={"insert_deduplicate": 0})
            if method == "raw_stream":
                consume_stream(result)
        except DatabaseError as ex:
            error = ex

        rows = call(param_client.query, "SELECT key FROM sql_write_retry").result_rows
        assert rows == [(13,)]
        assert proxy.statuses == [200]
        assert isinstance(error, OperationalError if fault == "disconnect" else DatabaseError)


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
        assert isinstance(error, OperationalError if fault == "disconnect" else DatabaseError)


@pytest.mark.parametrize("fault", _FAULTS)
@pytest.mark.parametrize("method", ["command", "query", "raw_query", "raw_stream"])
def test_read_retries_after_delivery(retry_proxy, call, consume_stream, method, fault):
    proxy, client = retry_proxy
    # Commands retain their remote-close retry but have no HTTP status retry budget.
    proxy.arm(fault)
    if method == "command" and fault != "disconnect":
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
def test_insert_data_retries_after_delivery(retry_proxy, param_client, call, table_context, method, deduplicate):
    if method == "insert_df" and pd is None:
        pytest.skip("pandas not available")
    proxy, client = retry_proxy
    table_settings = {"non_replicated_deduplication_window": 100} if deduplicate else None
    with table_context("insert_data_retry", ["key Int32"], settings=table_settings):
        proxy.arm("disconnect")
        settings = {"insert_deduplicate": int(deduplicate)}
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
