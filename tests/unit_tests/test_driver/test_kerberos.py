"""Unit tests for the use_kerberos auth mode (sync and async).

These avoid a live KDC and a live server: pyspnego itself is mocked (or, for the
"package not installed" cases, left as the real module-level None), so the tests
exercise validation and request/header wiring rather than a real GSSAPI handshake.
"""

import base64
from inspect import signature
from unittest.mock import MagicMock, patch

import pytest

import clickhouse_connect.driver as drv
import clickhouse_connect.driver.kerberos as kerberos_module
import clickhouse_connect.driver.options as options_module
from clickhouse_connect.driver import create_async_client, create_client
from clickhouse_connect.driver._backend.http_async import HttpAsyncBackend
from clickhouse_connect.driver._backend.http_sync import HttpSyncBackend
from clickhouse_connect.driver.asyncclient import AsyncClient
from clickhouse_connect.driver.client import Client
from clickhouse_connect.driver.exceptions import NotSupportedError, OperationalError, ProgrammingError
from clickhouse_connect.driver.httpclient import HttpClient


@pytest.fixture
def fake_spnego():
    """Patch clickhouse_connect.driver.options.spnego with a working fake module."""
    fake = MagicMock()
    fake.client.return_value.step.return_value = b"fake-token"
    with patch.object(options_module, "spnego", fake, create=True):
        yield fake


class TestKerberosValidation:
    def test_rejects_use_kerberos_with_username(self, fake_spnego):
        with pytest.raises(ProgrammingError):
            create_client(username="user_1", use_kerberos=True)

    def test_rejects_use_kerberos_with_password(self, fake_spnego):
        with pytest.raises(ProgrammingError):
            create_client(password="secret", use_kerberos=True)

    def test_rejects_use_kerberos_with_access_token(self, fake_spnego):
        with pytest.raises(ProgrammingError):
            create_client(access_token="t", use_kerberos=True)

    def test_rejects_use_kerberos_with_token_provider(self, fake_spnego):
        with pytest.raises(ProgrammingError):
            create_client(use_kerberos=True, token_provider=lambda: "t")

    def test_rejects_use_kerberos_with_client_cert(self, fake_spnego):
        with pytest.raises(ProgrammingError):
            create_client(use_kerberos=True, client_cert="cert.pem")

    @pytest.mark.asyncio
    async def test_async_rejects_use_kerberos_with_username(self, fake_spnego):
        with pytest.raises(ProgrammingError):
            await create_async_client(username="user_1", use_kerberos=True)

    @pytest.mark.asyncio
    async def test_async_rejects_use_kerberos_with_client_cert(self, fake_spnego):
        with pytest.raises(ProgrammingError):
            await create_async_client(use_kerberos=True, client_cert="cert.pem")

    def test_missing_pyspnego_raises_not_supported(self):
        with patch.object(options_module, "spnego", None, create=True):
            with pytest.raises(NotSupportedError):
                create_client(interface="http", host="h", port=8123, use_kerberos=True)

    @pytest.mark.asyncio
    async def test_async_missing_pyspnego_raises_not_supported(self):
        with patch.object(options_module, "spnego", None, create=True):
            with pytest.raises(NotSupportedError):
                await create_async_client(interface="http", host="h", port=8123, use_kerberos=True)


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
        use_kerberos=None,
        kerberos_hostname_override=None,
        settings=None,
        **kwargs,
    ):
        self.host = host
        self.use_kerberos = use_kerberos
        self.kerberos_hostname_override = kerberos_hostname_override
        self.extra = kwargs
        self.server_tz = None

    def _add_integration_tag(self, name):
        pass

    async def _initialize(self):
        pass


class TestKerberosConstruction:
    def test_direct_create_client(self):
        with patch.object(drv, "HttpClient", _RecordingClient):
            client = create_client(interface="http", host="h", port=8123, use_kerberos=True)
        assert client.use_kerberos is True

    def test_create_client_via_generic_args(self):
        with patch.object(drv, "HttpClient", _RecordingClient):
            client = create_client(interface="http", host="h", port=8123, generic_args={"use_kerberos": True})
        assert client.use_kerberos is True

    def test_kerberos_hostname_override_reaches_client(self):
        with patch.object(drv, "HttpClient", _RecordingClient):
            client = create_client(
                interface="http", host="h", port=8123, use_kerberos=True, kerberos_hostname_override="chnode1.example.com"
            )
        assert client.kerberos_hostname_override == "chnode1.example.com"

    @pytest.mark.asyncio
    async def test_create_async_client_via_generic_args(self):
        with patch("clickhouse_connect.driver.asyncclient.AsyncClient", _RecordingClient):
            client = await create_async_client(interface="http", host="h", port=8123, generic_args={"use_kerberos": True})
        assert client.use_kerberos is True

    def test_use_kerberos_in_httpclient_signature(self):
        # Guards the generic_args routing the construction tests depend on.
        assert "use_kerberos" in signature(HttpClient).parameters
        assert "kerberos_hostname_override" in signature(HttpClient).parameters

    def test_use_kerberos_in_asyncclient_signature(self):
        assert "use_kerberos" in signature(AsyncClient).parameters
        assert "kerberos_hostname_override" in signature(AsyncClient).parameters

    def test_real_http_client_sets_kerberos_hostname_from_host(self, fake_spnego):
        with patch.object(Client, "_init_common_settings"):
            client = HttpClient(interface="http", host="chnode1", port=8123, username="", password="", database=None, use_kerberos=True)
        assert client._backend.use_kerberos is True
        assert client._backend.kerberos_hostname == "chnode1"
        assert "Authorization" not in client._backend.headers

    def test_real_http_client_kerberos_hostname_override(self, fake_spnego):
        with patch.object(Client, "_init_common_settings"):
            client = HttpClient(
                interface="http",
                host="chnode1",
                port=8123,
                username="",
                password="",
                database=None,
                use_kerberos=True,
                kerberos_hostname_override="chnode1.example.com",
            )
        assert client._backend.kerberos_hostname == "chnode1.example.com"


class TestNegotiateAuthHeader:
    def test_builds_negotiate_header(self, fake_spnego):
        header = kerberos_module.negotiate_auth_header("chnode1.example.com")
        assert header == "Negotiate " + base64.b64encode(b"fake-token").decode()
        fake_spnego.client.assert_called_once_with(hostname="chnode1.example.com", service="HTTP")

    def test_missing_pyspnego(self):
        with patch.object(options_module, "spnego", None, create=True):
            with pytest.raises(NotSupportedError):
                kerberos_module.negotiate_auth_header("host")

    def test_negotiation_failure_wraps_and_preserves_pyspnego_message(self):
        # Real message captured from pyspnego with no 'gssapi'/'krb5' installed: its own Context
        # annotations already explain the actual cause, so it is surfaced as-is rather than reinterpreted.
        raw_message = (
            "SpnegoError (1): SpnegoError (16): Operation not supported or available, Context: No username "
            "or password was specified and the credential cache did not exist or contained no credentials, "
            "Context: Unable to negotiate common mechanism"
        )

        class _FakeSpnegoError(Exception):
            pass

        fake = MagicMock()
        fake.exceptions.SpnegoError = _FakeSpnegoError
        fake.client.return_value.step.side_effect = _FakeSpnegoError(raw_message)

        with patch.object(options_module, "spnego", fake, create=True):
            with pytest.raises(OperationalError) as exc_info:
                kerberos_module.negotiate_auth_header("chnode1.example.com")

        assert raw_message in str(exc_info.value)
        assert exc_info.value.__cause__ is not None  # original SpnegoError preserved via chaining


def _build_sync_kerberos_client(hostname="chnode1.example.com"):
    client = HttpClient.__new__(HttpClient)
    client._backend = HttpSyncBackend(
        url="http://localhost:8123",
        pool_manager=MagicMock(),
        owns_pool_manager=False,
        headers={},
        params={},
        timeout=None,
        server_host_name=None,
        token_provider=None,
        autogenerate_query_id=False,
        use_kerberos=True,
        kerberos_hostname=hostname,
    )
    client.url = "http://localhost:8123"
    client.params = client._backend.params
    return client


def _ok_response():
    r = MagicMock()
    r.status = 200
    r.headers = {}
    return r


class TestSyncKerberosRequest:
    def test_sends_fresh_negotiate_header_per_request(self):
        client = _build_sync_kerberos_client()
        sent_auth = []

        def fake_request(method, url, **kwargs):
            sent_auth.append(kwargs["headers"].get("Authorization"))
            return _ok_response()

        client.http = MagicMock()
        client.http.request = fake_request

        headers_seq = iter(["Negotiate aaa", "Negotiate bbb"])
        with patch("clickhouse_connect.driver._backend.http_sync.negotiate_auth_header", side_effect=lambda h: next(headers_seq)):
            client._raw_request(b"SELECT 1", {})
            client._raw_request(b"SELECT 2", {})

        assert sent_auth == ["Negotiate aaa", "Negotiate bbb"]

    def test_hostname_passed_to_negotiate(self):
        client = _build_sync_kerberos_client(hostname="override.example.com")
        client.http = MagicMock()
        client.http.request = MagicMock(return_value=_ok_response())

        with patch("clickhouse_connect.driver._backend.http_sync.negotiate_auth_header", return_value="Negotiate xyz") as mock_negotiate:
            client._raw_request(b"SELECT 1", {})

        mock_negotiate.assert_called_once_with("override.example.com")


class _FakeAsyncLease:
    def __init__(self, session):
        self.session = session
        self.inflight = 0

    def acquire(self):
        self.inflight += 1

    def release(self):
        self.inflight -= 1


class _FakeAsyncSession:
    def __init__(self, responses):
        self._seq = iter(responses)
        self.closed = False
        self.headers = {}
        self.sent_auth = []

    async def request(self, **kwargs):
        self.sent_auth.append(kwargs["headers"].get("Authorization"))
        return next(self._seq)


def _fake_async_response(status=200):
    r = MagicMock()
    r.status = status
    r.headers = {}

    async def _read():
        return b""

    r.read = _read
    r.close = MagicMock()
    return r


def _build_async_kerberos_client(hostname="chnode1.example.com", responses=None):
    client = AsyncClient.__new__(AsyncClient)
    client._backend = HttpAsyncBackend(
        url="http://localhost:8123",
        headers={},
        client_settings={},
        timeout=None,
        connector_kwargs={},
        ssl_context=None,
        proxy_url=None,
        server_host_name=None,
        token_provider=None,
        autogenerate_query_id=False,
        use_kerberos=True,
        kerberos_hostname=hostname,
    )
    session = _FakeAsyncSession(responses if responses is not None else [_fake_async_response(200)])
    client._backend.session_lease = _FakeAsyncLease(session)
    return client, session


class TestAsyncKerberosRequest:
    @pytest.mark.asyncio
    async def test_sends_negotiate_header(self):
        client, session = _build_async_kerberos_client()
        with patch("clickhouse_connect.driver._backend.http_async.negotiate_auth_header", return_value="Negotiate xyz") as mock_negotiate:
            resp = await client._raw_request(b"SELECT 1", {})
        assert resp.status == 200
        assert session.sent_auth == ["Negotiate xyz"]
        mock_negotiate.assert_called_once_with("chnode1.example.com")
