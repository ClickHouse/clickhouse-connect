from io import BytesIO
from types import SimpleNamespace
from unittest.mock import Mock
from urllib.parse import urlencode, urlsplit

import pytest

from clickhouse_connect.driver.models import SettingDef
from tests.integration_tests.conftest import nullable_tuple_settings
from tests.integration_tests.test_http_retries import _RetryHandler, _RetryProxy


@pytest.mark.parametrize("value", ["0", "1"])
@pytest.mark.parametrize("readonly", [0, 1])
def test_nullable_tuple_settings_available(value, readonly):
    name = "allow_experimental_nullable_tuple_type"
    client = SimpleNamespace(server_settings={name: SettingDef(name, value, readonly)})

    assert nullable_tuple_settings(client) == {name: 1}


def test_nullable_tuple_settings_unavailable():
    client = SimpleNamespace(server_settings={})

    with pytest.raises(pytest.skip.Exception, match=r"Server does not support Nullable\(Tuple"):
        nullable_tuple_settings(client)


@pytest.mark.parametrize("fault", ["disconnect", "progress_headers", "status_only", 429, 503, 504, None])
def test_retry_proxy_waits_for_query_release_before_fault(monkeypatch, fault):
    proxy = _RetryProxy.__new__(_RetryProxy)
    proxy.upstream = urlsplit("http://localhost:8123")
    proxy.observer = Mock()
    proxy.observer.command.side_effect = [1, 1, 0]
    proxy.statuses = []
    proxy.fault = fault
    query_id = "user_1 + '&"
    handler = SimpleNamespace(
        server=proxy,
        path=f"/?{urlencode({'query_id': query_id})}",
        headers={},
        rfile=BytesIO(),
        wfile=BytesIO(),
        connection=Mock(),
        send_response=Mock(),
        send_header=Mock(),
        end_headers=Mock(),
    )
    upstream = Mock()
    upstream.getresponse.return_value = SimpleNamespace(status=200, read=lambda: b"13\n", getheaders=lambda: [])
    monkeypatch.setattr("tests.integration_tests.test_http_retries.HTTPConnection", Mock(return_value=upstream))
    sleep = Mock()
    monkeypatch.setattr("tests.integration_tests.test_http_retries.time.sleep", sleep)

    def assert_query_released(*_args):
        assert proxy.observer.command.call_count == (3 if fault is not None else 0)

    handler.send_response.side_effect = assert_query_released
    handler.connection.shutdown.side_effect = assert_query_released
    handler.wfile.flush = Mock(side_effect=assert_query_released)
    _RetryHandler.do_POST(handler)

    assert proxy.statuses == [200]
    assert proxy.fault is None
    assert proxy.observer.command.call_count == (3 if fault is not None else 0)
    assert sleep.call_count == (2 if fault is not None else 0)
    for args, kwargs in proxy.observer.command.call_args_list:
        assert args == ("SELECT count() FROM system.processes WHERE query_id = {query_id:String}",)
        assert kwargs == {"parameters": {"query_id": query_id}}


def test_retry_proxy_query_release_timeout(monkeypatch):
    proxy = _RetryProxy.__new__(_RetryProxy)
    proxy.observer = Mock()
    proxy.observer.command.return_value = 1
    monkeypatch.setattr("tests.integration_tests.test_http_retries.time.monotonic", Mock(side_effect=[0, 10]))

    with pytest.raises(AssertionError, match="Query user_1 is still in system.processes after 10 seconds"):
        proxy.wait_for_query("/?query_id=user_1")

    proxy.observer.command.assert_called_once()
