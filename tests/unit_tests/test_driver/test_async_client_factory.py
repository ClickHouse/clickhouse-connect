from typing import Any
from unittest.mock import AsyncMock, patch
from urllib.parse import urlencode

import pytest

from clickhouse_connect.driver import create_async_client, create_client
from clickhouse_connect.driver.asyncclient import AsyncClient
from clickhouse_connect.driver.exceptions import ProgrammingError
from clickhouse_connect.driver.httpclient import HttpClient

_BOOL_OPTIONS = (
    "autogenerate_query_id",
    "autogenerate_session_id",
    "form_encode_query_params",
)
_BOOL_CASES = (
    ("true", True),
    (" TRUE ", True),
    ("1", True),
    ("y", True),
    ("yes", True),
    ("on", True),
    ("false", False),
    (" FALSE ", False),
    ("0", False),
    ("n", False),
    ("no", False),
    ("off", False),
)
_SYNC_NUMERIC_OPTIONS = (
    "connect_timeout",
    "query_limit",
    "query_retries",
    "send_receive_timeout",
)
_ASYNC_NUMERIC_OPTIONS = (
    "connect_timeout",
    "connector_limit",
    "connector_limit_per_host",
    "keepalive_timeout",
    "query_limit",
    "query_retries",
    "send_receive_timeout",
)
_SOURCE_CONNECTOR_OPTIONS = {
    "connector_limit": "13",
    "connector_limit_per_host": "7",
    "keepalive_timeout": "4.5",
}
_CONNECTOR_CASES = (
    ("connector_limit", "limit", 79),
    ("connector_limit_per_host", "limit_per_host", 71),
    ("keepalive_timeout", "keepalive_timeout", 17.5),
)


def _source_kwargs(source: str, options: dict[str, Any]) -> dict[str, Any]:
    if source == "dsn":
        return {"dsn": f"http://localhost:8123/default?{urlencode(options)}"}
    return {"host": "localhost", "generic_args": options}


async def _create_factory_client(
    client_kind: str,
    source: str,
    options: dict[str, Any],
    **factory_options: Any,
) -> Any:
    kwargs = _source_kwargs(source, options)
    kwargs.update(factory_options)
    if client_kind == "sync":
        with patch.object(HttpClient, "_init_common_settings", autospec=True):
            return create_client(**kwargs)
    with patch.object(AsyncClient, "_initialize", new=AsyncMock()):
        return await create_async_client(**kwargs)


@pytest.mark.parametrize("value,expected", _BOOL_CASES)
@pytest.mark.parametrize("client_kind", ("sync", "async"))
@pytest.mark.parametrize("source", ("dsn", "generic"))
@pytest.mark.asyncio
async def test_boolean_options_are_coerced(value, expected, client_kind, source):
    client = await _create_factory_client(client_kind, source, dict.fromkeys(_BOOL_OPTIONS, value))

    if client_kind == "sync":
        assert ("session_id" in client._ch_settings) is expected
    else:
        assert client._autogenerate_session_id_param is expected
    assert client._backend.autogenerate_query_id is expected
    assert client.form_encode_query_params is expected


@pytest.mark.parametrize("value", (True, False))
@pytest.mark.parametrize("client_kind", ("sync", "async"))
@pytest.mark.asyncio
async def test_native_boolean_options_are_preserved(value, client_kind):
    client = await _create_factory_client(client_kind, "generic", dict.fromkeys(_BOOL_OPTIONS, value))

    if client_kind == "sync":
        assert ("session_id" in client._ch_settings) is value
    else:
        assert client._autogenerate_session_id_param is value
    assert client._backend.autogenerate_query_id is value
    assert client.form_encode_query_params is value


@pytest.mark.parametrize("source", ("dsn", "generic"))
@pytest.mark.asyncio
async def test_sync_numeric_options_are_coerced(source):
    client = await _create_factory_client(
        "sync",
        source,
        {
            "connect_timeout": "11",
            "query_limit": "17",
            "query_retries": "3",
            "send_receive_timeout": "19",
        },
    )

    assert client.timeout.connect_timeout == 11
    assert client.timeout.read_timeout == 19
    assert client.query_limit == 17
    assert client.query_retries == 3
    assert type(client.timeout.connect_timeout) is int
    assert type(client.timeout.read_timeout) is int


@pytest.mark.parametrize("source", ("dsn", "generic"))
@pytest.mark.asyncio
async def test_async_numeric_options_are_coerced(source):
    client = await _create_factory_client(
        "async",
        source,
        {
            "connect_timeout": "11.25",
            "connector_limit": "13",
            "connector_limit_per_host": "7",
            "keepalive_timeout": "4.5",
            "query_limit": "17",
            "query_retries": "3",
            "send_receive_timeout": "19.5",
        },
    )

    assert client._timeout.connect == 11.25
    assert client._timeout.sock_connect == 11.25
    assert client._timeout.sock_read == 19.5
    assert client.query_limit == 17
    assert client.query_retries == 3
    assert client._backend.connector_kwargs["limit"] == 13
    assert client._backend.connector_kwargs["limit_per_host"] == 7
    assert client._backend.connector_kwargs["keepalive_timeout"] == 4.5
    assert type(client.query_limit) is int
    assert type(client.query_retries) is int
    assert type(client._backend.connector_kwargs["limit"]) is int
    assert type(client._backend.connector_kwargs["limit_per_host"]) is int
    assert type(client._backend.connector_kwargs["keepalive_timeout"]) is float


@pytest.mark.parametrize("source", ("dsn", "generic"))
@pytest.mark.asyncio
async def test_named_connector_options_take_precedence(source):
    client = await _create_factory_client(
        "async",
        source,
        _SOURCE_CONNECTOR_OPTIONS,
        connector_limit=79,
        connector_limit_per_host=71,
        keepalive_timeout=17.5,
    )

    assert client._backend.connector_kwargs["limit"] == 79
    assert client._backend.connector_kwargs["limit_per_host"] == 71
    assert client._backend.connector_kwargs["keepalive_timeout"] == 17.5


@pytest.mark.parametrize("source", ("dsn", "generic"))
@pytest.mark.asyncio
async def test_none_connector_options_fall_through_to_secondary_source(source):
    client = await _create_factory_client(
        "async",
        source,
        _SOURCE_CONNECTOR_OPTIONS,
        connector_limit=None,
        connector_limit_per_host=None,
        keepalive_timeout=None,
    )

    assert client._backend.connector_kwargs["limit"] == 13
    assert client._backend.connector_kwargs["limit_per_host"] == 7
    assert client._backend.connector_kwargs["keepalive_timeout"] == 4.5


@pytest.mark.parametrize(
    "factory_options",
    (
        {},
        {"connector_limit": None, "connector_limit_per_host": None, "keepalive_timeout": None},
    ),
)
@pytest.mark.asyncio
async def test_connector_options_fall_back_to_defaults(factory_options):
    with patch.object(AsyncClient, "_initialize", new=AsyncMock()):
        client = await create_async_client(host="localhost", **factory_options)

    assert client._backend.connector_kwargs["limit"] == 100
    assert client._backend.connector_kwargs["limit_per_host"] == 20
    assert client._backend.connector_kwargs["keepalive_timeout"] == 30.0


@pytest.mark.asyncio
async def test_generic_connector_options_take_precedence_over_dsn():
    dsn_options = urlencode(_SOURCE_CONNECTOR_OPTIONS)
    with patch.object(AsyncClient, "_initialize", new=AsyncMock()):
        client = await create_async_client(
            dsn=f"http://localhost:8123/default?{dsn_options}",
            generic_args={
                "connector_limit": "17",
                "connector_limit_per_host": "11",
                "keepalive_timeout": "8.5",
            },
        )

    assert client._backend.connector_kwargs["limit"] == 17
    assert client._backend.connector_kwargs["limit_per_host"] == 11
    assert client._backend.connector_kwargs["keepalive_timeout"] == 8.5


@pytest.mark.asyncio
async def test_none_generic_connector_options_fall_through_to_dsn():
    dsn_options = urlencode(_SOURCE_CONNECTOR_OPTIONS)
    with patch.object(AsyncClient, "_initialize", new=AsyncMock()):
        client = await create_async_client(
            dsn=f"http://localhost:8123/default?{dsn_options}",
            generic_args=dict.fromkeys(_SOURCE_CONNECTOR_OPTIONS),
            connector_limit=None,
            connector_limit_per_host=None,
            keepalive_timeout=None,
        )

    assert client._backend.connector_kwargs["limit"] == 13
    assert client._backend.connector_kwargs["limit_per_host"] == 7
    assert client._backend.connector_kwargs["keepalive_timeout"] == 4.5


@pytest.mark.parametrize("source", ("dsn", "generic"))
@pytest.mark.parametrize("option_name,backend_name,explicit_value", _CONNECTOR_CASES)
@pytest.mark.asyncio
async def test_explicit_connector_options_suppress_invalid_lower_priority_values(source, option_name, backend_name, explicit_value):
    client = await _create_factory_client(
        "async",
        source,
        {option_name: "invalid"},
        **{option_name: explicit_value},
    )

    assert client._backend.connector_kwargs[backend_name] == explicit_value


@pytest.mark.parametrize("option_name,backend_name,generic_value", _CONNECTOR_CASES)
@pytest.mark.asyncio
async def test_generic_connector_options_suppress_invalid_dsn(option_name, backend_name, generic_value):
    with patch.object(AsyncClient, "_initialize", new=AsyncMock()):
        client = await create_async_client(
            dsn=f"http://localhost:8123/default?{urlencode({option_name: 'invalid'})}",
            generic_args={option_name: str(generic_value)},
        )

    assert client._backend.connector_kwargs[backend_name] == generic_value


@pytest.mark.parametrize("source", ("dsn", "generic"))
@pytest.mark.parametrize("client_kind", ("sync", "async"))
@pytest.mark.parametrize("name", _BOOL_OPTIONS)
@pytest.mark.asyncio
async def test_invalid_boolean_options_raise_programming_error(source, client_kind, name):
    bad_value = "sometimes"
    with pytest.raises(ProgrammingError) as exc_info:
        await _create_factory_client(client_kind, source, {name: bad_value})

    assert name in str(exc_info.value)
    assert repr(bad_value) in str(exc_info.value)


@pytest.mark.parametrize("source", ("dsn", "generic"))
@pytest.mark.parametrize("name", _SYNC_NUMERIC_OPTIONS)
@pytest.mark.asyncio
async def test_invalid_sync_numeric_options_raise_programming_error(source, name):
    bad_value = "several"
    with pytest.raises(ProgrammingError) as exc_info:
        await _create_factory_client("sync", source, {name: bad_value})

    assert name in str(exc_info.value)
    assert repr(bad_value) in str(exc_info.value)


@pytest.mark.parametrize("source", ("dsn", "generic"))
@pytest.mark.parametrize("name", _ASYNC_NUMERIC_OPTIONS)
@pytest.mark.asyncio
async def test_invalid_async_numeric_options_raise_programming_error(source, name):
    bad_value = "several"
    with pytest.raises(ProgrammingError) as exc_info:
        await _create_factory_client("async", source, {name: bad_value})

    assert name in str(exc_info.value)
    assert repr(bad_value) in str(exc_info.value)
