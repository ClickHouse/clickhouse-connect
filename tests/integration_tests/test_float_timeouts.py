"""Regression tests: the sync HTTP client must accept fractional-second timeouts (issue #1245)."""

import os

import pytest

import clickhouse_connect
from clickhouse_connect.driver.exceptions import OperationalError

HOST = os.environ.get("CLICKHOUSE_CONNECT_TEST_HOST", "localhost")
PORT = int(os.environ.get("CLICKHOUSE_CONNECT_TEST_PORT", "8123"))


def _skip_if_no_server():
    try:
        clickhouse_connect.get_client(host=HOST, port=PORT).close()
    except Exception:  # pylint: disable=broad-except
        pytest.skip(f"No ClickHouse server reachable at {HOST}:{PORT}")


def test_string_timeouts_from_dsn_become_floats():
    from clickhouse_connect.driver import _SYNC_FLOAT_OPTIONS, _SYNC_INT_OPTIONS, _coerce_numeric_options  # pylint: disable=import-outside-toplevel

    options = {"connect_timeout": "0.3", "send_receive_timeout": "0.5", "query_limit": "5"}
    _coerce_numeric_options(options, _SYNC_INT_OPTIONS, _SYNC_FLOAT_OPTIONS)
    assert options == {"connect_timeout": 0.3, "send_receive_timeout": 0.5, "query_limit": 5}


def test_client_accepts_fractional_timeouts():
    _skip_if_no_server()
    client = clickhouse_connect.get_client(host=HOST, port=PORT, connect_timeout=0.5, send_receive_timeout=0.5)
    try:
        assert client.query("SELECT 1").result_rows == [(1,)]
    finally:
        client.close()


def test_fractional_read_timeout_is_enforced():
    _skip_if_no_server()
    client = clickhouse_connect.get_client(host=HOST, port=PORT, send_receive_timeout=0.3)
    try:
        with pytest.raises(OperationalError):
            client.query("SELECT sleep(2)")
    finally:
        client.close()
