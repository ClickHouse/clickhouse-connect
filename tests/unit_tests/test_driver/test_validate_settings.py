"""Unit tests for Client._validate_settings / _validate_setting.

Covers readonly vs unknown (custom) settings, including the CHANGEABLE_IN_READONLY
case from issue #530 where custom settings are not present in system.settings for
the connecting user, plus the reserved transport parameter names that must never be
forwarded as settings.
"""

from __future__ import annotations

from unittest.mock import patch

import pytest

from clickhouse_connect import common
from clickhouse_connect.driver.exceptions import ProgrammingError
from clickhouse_connect.driver.httpclient import HttpClient
from clickhouse_connect.driver.models import SettingDef


@pytest.fixture
def client_with_settings():
    """Build HttpClients whose server settings are stubbed, closing them on teardown."""
    clients: list[HttpClient] = []

    def factory(server_settings: dict[str, SettingDef]) -> HttpClient:
        with patch.object(HttpClient, "_init_common_settings", autospec=True):
            client = HttpClient(
                interface="http",
                host="localhost",
                port=8123,
                username="default",
                password="",
                database="default",
            )
        client.server_settings = server_settings
        clients.append(client)
        return client

    yield factory

    for client in clients:
        client.close()


@pytest.fixture(autouse=True)
def _restore_invalid_setting_action():
    original = common.get_setting("invalid_setting_action")
    try:
        yield
    finally:
        common.set_setting("invalid_setting_action", original)


def test_unknown_custom_setting_is_sent_by_default(client_with_settings):
    """Custom settings absent from system.settings must be forwarded (issue #530)."""
    client = client_with_settings({})
    common.set_setting("invalid_setting_action", "error")

    assert client._validate_settings({"SQL_RO_my_rls_key": "tenant_1"}) == {"SQL_RO_my_rls_key": "tenant_1"}


def test_unknown_custom_setting_is_sent_with_send_action(client_with_settings):
    client = client_with_settings({})
    common.set_setting("invalid_setting_action", "send")

    assert client._validate_settings({"SQL_RO_my_rls_key": "tenant_1"}) == {"SQL_RO_my_rls_key": "tenant_1"}


def test_unknown_custom_setting_is_dropped_with_drop_action(client_with_settings, caplog):
    """invalid_setting_action='drop' keeps a settings dict portable across server versions."""
    client = client_with_settings({})
    common.set_setting("invalid_setting_action", "drop")

    with caplog.at_level("WARNING"):
        validated = client._validate_settings({"SQL_RO_my_rls_key": "tenant_1"})

    assert validated == {}
    assert any("SQL_RO_my_rls_key" in r.message for r in caplog.records)


def test_known_writable_setting_is_sent(client_with_settings):
    client = client_with_settings({"max_threads": SettingDef("max_threads", "8", 0)})
    common.set_setting("invalid_setting_action", "error")

    assert client._validate_settings({"max_threads": "4"}) == {"max_threads": "4"}


def test_known_readonly_setting_raises_by_default(client_with_settings):
    client = client_with_settings({"readonly": SettingDef("readonly", "1", 1)})
    common.set_setting("invalid_setting_action", "error")

    with pytest.raises(ProgrammingError, match="Setting readonly is readonly"):
        client._validate_settings({"readonly": "0"})


def test_known_readonly_matching_value_is_skipped(client_with_settings):
    """Matching readonly values are not re-sent (issue #469 / #639 behavior)."""
    client = client_with_settings({"readonly": SettingDef("readonly", "1", 1)})
    common.set_setting("invalid_setting_action", "error")

    assert client._validate_settings({"readonly": "1"}) == {}


def test_known_readonly_send_action_forwards_with_warning(client_with_settings, caplog):
    client = client_with_settings({"max_memory_usage": SettingDef("max_memory_usage", "0", 1)})
    common.set_setting("invalid_setting_action", "send")

    with caplog.at_level("WARNING"):
        validated = client._validate_settings({"max_memory_usage": "1000"})

    assert validated == {"max_memory_usage": "1000"}
    assert any("readonly setting max_memory_usage" in r.message for r in caplog.records)


def test_known_readonly_drop_action_drops(client_with_settings, caplog):
    client = client_with_settings({"max_memory_usage": SettingDef("max_memory_usage", "0", 1)})
    common.set_setting("invalid_setting_action", "drop")

    with caplog.at_level("WARNING"):
        validated = client._validate_settings({"max_memory_usage": "1000"})

    assert validated == {}
    assert any("Dropping readonly setting max_memory_usage" in r.message for r in caplog.records)


@pytest.mark.parametrize(
    "reserved",
    ["query", "user", "password", "default_format", "stacktrace", "close_session", "param_id", "param_"],
)
def test_reserved_transport_name_raises(client_with_settings, reserved):
    """Reserved HTTP request parameters are never forwarded as settings, even when unknown.

    param_* is the bound-parameter namespace, so a setting there would override a real query parameter.
    """
    client = client_with_settings({})
    common.set_setting("invalid_setting_action", "error")

    with pytest.raises(ProgrammingError, match="reserved transport parameter"):
        client._validate_settings({reserved: "x"})


@pytest.mark.parametrize("action", ["drop", "send"])
def test_reserved_transport_name_raises_regardless_of_action(client_with_settings, action):
    """A reserved name cannot be dropped or sent, since forwarding it would corrupt the request."""
    client = client_with_settings({})
    common.set_setting("invalid_setting_action", action)

    with pytest.raises(ProgrammingError, match="reserved transport parameter"):
        client._validate_settings({"query": "SELECT 1"})


def test_optional_transport_setting_unknown_is_dropped(client_with_settings):
    client = client_with_settings({})
    common.set_setting("invalid_setting_action", "error")

    assert client._validate_settings({"enable_http_compression": "1"}) == {}


def test_transport_settings_always_pass_through(client_with_settings):
    client = client_with_settings({})
    common.set_setting("invalid_setting_action", "error")

    assert client._validate_settings({"query_id": "q-530"}) == {"query_id": "q-530"}
