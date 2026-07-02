"""Unit tests for the token_provider auth mode (sync and async).

These avoid a live server: validation runs before any connection, construction
is exercised with a recording stand-in for the client, and the auth-refresh
retry loop is driven against a fake transport on a client built via __new__.
"""

import asyncio
from inspect import signature
from unittest.mock import MagicMock, patch

import pytest

import clickhouse_connect.driver as drv
from clickhouse_connect import dbapi
from clickhouse_connect.driver import create_async_client, create_client
from clickhouse_connect.driver.asyncclient import AsyncClient
from clickhouse_connect.driver.exceptions import DatabaseError, ProgrammingError
from clickhouse_connect.driver.httpclient import HttpClient, auth_failed_ex_code


class _TokenSequence:
    """Callable token_provider returning preset tokens, recording call count."""

    def __init__(self, *tokens):
        self._tokens = list(tokens) or ["tok"]
        self.calls = 0

    def __call__(self):
        token = self._tokens[min(self.calls, len(self._tokens) - 1)]
        self.calls += 1
        return token


class TestTokenProviderValidation:
    def test_rejects_token_provider_with_username(self):
        with pytest.raises(ProgrammingError):
            create_client(username="user_1", token_provider=lambda: "t")

    def test_rejects_token_provider_with_password(self):
        with pytest.raises(ProgrammingError):
            create_client(password="secret", token_provider=lambda: "t")

    def test_rejects_token_provider_with_access_token(self):
        with pytest.raises(ProgrammingError):
            create_client(access_token="t", token_provider=lambda: "t")

    @pytest.mark.asyncio
    async def test_async_rejects_token_provider_with_username(self):
        with pytest.raises(ProgrammingError):
            await create_async_client(username="user_1", token_provider=lambda: "t")

    def test_rejects_generic_args_token_provider_with_username(self):
        with pytest.raises(ProgrammingError):
            create_client(interface="http", host="h", port=8123, username="user_1", generic_args={"token_provider": lambda: "t"})

    def test_rejects_generic_args_token_provider_with_access_token(self):
        with pytest.raises(ProgrammingError):
            create_client(interface="http", host="h", port=8123, access_token="t", generic_args={"token_provider": lambda: "t"})

    @pytest.mark.asyncio
    async def test_async_rejects_generic_args_token_provider_with_username(self):
        with pytest.raises(ProgrammingError):
            await create_async_client(
                interface="http", host="h", port=8123, username="user_1", generic_args={"token_provider": lambda: "t"}
            )


class _RecordingClient:
    """Stand-in mirroring the leading client signature so generic_args routing works."""

    def __init__(
        self,
        interface=None,
        host=None,
        port=None,
        username=None,
        password=None,
        database=None,
        access_token=None,
        token_provider=None,
        settings=None,
        **kwargs,
    ):
        self.access_token = access_token
        self.token_provider = token_provider
        self.extra = kwargs
        self.server_tz = None

    def _add_integration_tag(self, name):
        pass

    async def _initialize(self):
        pass


class TestTokenProviderConstruction:
    """token_provider must reach the client through every entry point without colliding."""

    def test_direct_create_client(self):
        fn = lambda: "tok"  # noqa: E731
        with patch.object(drv, "HttpClient", _RecordingClient):
            client = create_client(interface="http", host="h", port=8123, token_provider=fn)
        assert client.token_provider is fn

    def test_create_client_via_generic_args(self):
        fn = lambda: "tok"  # noqa: E731
        with patch.object(drv, "HttpClient", _RecordingClient):
            client = create_client(interface="http", host="h", port=8123, generic_args={"token_provider": fn})
        assert client.token_provider is fn
        assert "token_provider" not in client.extra

    def test_access_token_via_generic_args(self):
        with patch.object(drv, "HttpClient", _RecordingClient):
            client = create_client(interface="http", host="h", port=8123, generic_args={"access_token": "jwt"})
        assert client.access_token == "jwt"
        assert "access_token" not in client.extra

    def test_dbapi_connect(self):
        fn = lambda: "tok"  # noqa: E731
        with patch.object(drv, "HttpClient", _RecordingClient):
            conn = dbapi.connect(token_provider=fn, host="h", port=8123, interface="http")
        assert conn.client.token_provider is fn

    @pytest.mark.asyncio
    async def test_create_async_client_via_generic_args(self):
        fn = lambda: "tok"  # noqa: E731
        with patch("clickhouse_connect.driver.asyncclient.AsyncClient", _RecordingClient):
            client = await create_async_client(interface="http", host="h", port=8123, generic_args={"token_provider": fn})
        assert client.token_provider is fn
        assert "token_provider" not in client.extra

    def test_explicit_and_generic_args_access_token_no_duplicate(self):
        with patch.object(drv, "HttpClient", _RecordingClient):
            client = create_client(interface="http", host="h", port=8123, access_token="a", generic_args={"access_token": "b"})
        assert client.access_token == "a"  # explicit wins, no duplicate keyword
        assert "access_token" not in client.extra

    def test_explicit_and_generic_args_token_provider_no_duplicate(self):
        explicit = lambda: "a"  # noqa: E731
        with patch.object(drv, "HttpClient", _RecordingClient):
            client = create_client(
                interface="http", host="h", port=8123, token_provider=explicit, generic_args={"token_provider": lambda: "b"}
            )
        assert client.token_provider is explicit  # explicit wins, no duplicate keyword
        assert "token_provider" not in client.extra

    def test_token_provider_in_httpclient_signature(self):
        # Guards the routing the construction tests depend on.
        assert "token_provider" in signature(HttpClient).parameters


def _fake_response(status, ex_code=None):
    r = MagicMock()
    r.status = status
    r.headers = {} if ex_code is None else {"X-ClickHouse-Exception-Code": ex_code}
    r.data = b""
    r.close = MagicMock()
    return r


def _build_sync_client(provider):
    client = HttpClient.__new__(HttpClient)
    client._token_provider = provider
    client.headers = {"Authorization": f"Bearer {provider()}"}  # initial token, mirrors __init__
    client.url = "http://localhost:8123"
    client.params = {}
    client.timeout = None
    client.http_retries = 1
    client.server_host_name = None
    client._autogenerate_query_id = False
    client._send_progress = None
    client._progress_interval = None
    client._active_session = None
    client.show_clickhouse_errors = True
    return client


def _wire_sync(client, responses):
    sent_auth = []
    seq = iter(responses)

    def fake_request(method, url, **kwargs):
        sent_auth.append(kwargs["headers"].get("Authorization"))
        return next(seq)

    client.http = MagicMock()
    client.http.request = fake_request
    return sent_auth


class TestSyncAuthRetry:
    def test_refresh_on_516_then_success(self):
        provider = _TokenSequence("init", "refreshed")
        client = _build_sync_client(provider)
        sent_auth = _wire_sync(client, [_fake_response(500, auth_failed_ex_code), _fake_response(200)])
        resp = client._raw_request(b"SELECT 1", {})
        assert resp.status == 200
        assert provider.calls == 2  # initial token plus one refresh
        assert sent_auth == ["Bearer init", "Bearer refreshed"]

    def test_at_most_one_refresh(self):
        provider = _TokenSequence("init", "refreshed", "stillbad")
        client = _build_sync_client(provider)
        _wire_sync(client, [_fake_response(500, auth_failed_ex_code), _fake_response(500, auth_failed_ex_code)])
        with pytest.raises(DatabaseError):
            client._raw_request(b"SELECT 1", {})
        assert provider.calls == 2  # refreshed once, second failure surfaced

    def test_non_replayable_body_not_retried(self):
        provider = _TokenSequence("init", "refreshed")
        client = _build_sync_client(provider)
        _wire_sync(client, [_fake_response(500, auth_failed_ex_code)])
        gen = (b"row" for _ in range(1))
        with pytest.raises(DatabaseError):
            client._raw_request(gen, {})  # generator body, no retry_body
        assert provider.calls == 1  # never refreshed
        assert next(gen) == b"row"  # body untouched, not consumed by a retry

    def test_replayable_body_with_retry_body_refreshes(self):
        provider = _TokenSequence("init", "refreshed")
        client = _build_sync_client(provider)
        _wire_sync(client, [_fake_response(500, auth_failed_ex_code), _fake_response(200)])
        gen = (b"row" for _ in range(1))
        resp = client._raw_request(gen, {}, retry_body=lambda: (b"row" for _ in range(1)))
        assert resp.status == 200
        assert provider.calls == 2

    def test_no_provider_surfaces_immediately(self):
        client = _build_sync_client(_TokenSequence("init"))
        client._token_provider = None
        _wire_sync(client, [_fake_response(500, auth_failed_ex_code)])
        with pytest.raises(DatabaseError):
            client._raw_request(b"SELECT 1", {})


def _fake_async_response(status, ex_code=None):
    r = MagicMock()
    r.status = status
    r.headers = {} if ex_code is None else {"X-ClickHouse-Exception-Code": ex_code}

    async def _read():
        return b""

    r.read = _read
    r.close = MagicMock()
    return r


class _FakeLease:
    def __init__(self, session):
        self.session = session
        self.inflight = 0

    def acquire(self):
        self.inflight += 1

    def release(self):
        self.inflight -= 1


class _FakeSession:
    def __init__(self, responses):
        self._seq = iter(responses)
        self.closed = False
        self.headers = {}
        self.sent_auth = []

    async def request(self, **kwargs):
        self.sent_auth.append(kwargs["headers"].get("Authorization"))
        return next(self._seq)


def _build_async_client(provider, responses):
    client = AsyncClient.__new__(AsyncClient)
    client._token_provider = provider
    client.headers = {"Authorization": f"Bearer {provider()}"}
    client.url = "http://localhost:8123"
    client.server_host_name = None
    client._client_settings = {}
    client._send_progress = None
    client._progress_interval = None
    client._autogenerate_query_id = False
    client._active_session = None
    client._last_pool_reset = None
    client.show_clickhouse_errors = True
    client._session_lock = asyncio.Lock()
    session = _FakeSession(responses)
    client._session_lease = _FakeLease(session)
    return client, session


class TestAsyncAuthRetry:
    @pytest.mark.asyncio
    async def test_refresh_on_516_then_success(self):
        provider = _TokenSequence("init", "refreshed")
        client, session = _build_async_client(provider, [_fake_async_response(500, auth_failed_ex_code), _fake_async_response(200)])
        resp = await client._raw_request(b"SELECT 1", {})
        assert resp.status == 200
        assert provider.calls == 2
        assert session.sent_auth == ["Bearer init", "Bearer refreshed"]
        assert client._session_lease.inflight == 1  # held for the caller until the body is consumed
        resp._lease_release()
        assert client._session_lease.inflight == 0  # released after the retry, no leak

    @pytest.mark.asyncio
    async def test_non_replayable_body_not_retried(self):
        provider = _TokenSequence("init", "refreshed")
        client, _ = _build_async_client(provider, [_fake_async_response(500, auth_failed_ex_code)])
        gen = (b"row" for _ in range(1))
        with pytest.raises(DatabaseError):
            await client._raw_request(gen, {})
        assert provider.calls == 1
        assert next(gen) == b"row"
        assert client._session_lease.inflight == 0  # lease released even on the surfaced error
