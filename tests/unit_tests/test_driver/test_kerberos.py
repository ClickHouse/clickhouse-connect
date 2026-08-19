"""Unit tests for the use_kerberos auth mode (sync and async).

These avoid a live KDC and a live server: pyspnego itself is mocked (or, for the
"package not installed" cases, left as the real module-level None), so the tests
exercise validation and request/header wiring rather than a real GSSAPI handshake.
"""

import base64
from inspect import signature
from unittest.mock import MagicMock, call, patch

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

    def test_httpclient_signature_preserves_positional_parameters(self):
        parameters = list(signature(HttpClient).parameters)
        assert parameters[8] == "compress"
        assert parameters[-2:] == ["use_kerberos", "kerberos_hostname_override"]

    def test_asyncclient_signature_preserves_positional_parameters(self):
        parameters = list(signature(AsyncClient).parameters)
        assert parameters[8] == "compress"
        assert parameters[-2:] == ["use_kerberos", "kerberos_hostname_override"]

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

    def test_direct_httpclient_rejects_kerberos_with_password(self, fake_spnego):
        with pytest.raises(ProgrammingError):
            HttpClient("http", "chnode1", 8123, "", "secret", None, use_kerberos=True)

    def test_direct_httpclient_rejects_provider_without_calling_it(self, fake_spnego):
        provider = MagicMock(return_value="token")

        with pytest.raises(ProgrammingError):
            HttpClient("http", "chnode1", 8123, "", "", None, token_provider=provider, use_kerberos=True)

        provider.assert_not_called()

    def test_direct_asyncclient_rejects_kerberos_with_password(self, fake_spnego):
        with pytest.raises(ProgrammingError):
            AsyncClient("http", "chnode1", 8123, "", "secret", None, use_kerberos=True)


class TestKerberosAuthContext:
    def test_builds_kerberos_negotiate_header(self, fake_spnego):
        context = kerberos_module.KerberosAuthContext("chnode1.example.com")

        assert context.authorization_header == "Negotiate " + base64.b64encode(b"fake-token").decode()
        fake_spnego.client.assert_called_once_with(
            hostname="chnode1.example.com",
            service="HTTP",
            protocol="kerberos",
        )

    def test_validates_server_token_with_same_context(self, fake_spnego):
        spnego_context = fake_spnego.client.return_value
        spnego_context.step.side_effect = [b"request-token", None]
        spnego_context.complete = True
        context = kerberos_module.KerberosAuthContext("chnode1.example.com")

        context.validate_response("Negotiate " + base64.b64encode(b"response-token").decode())

        assert spnego_context.step.call_args_list == [call(), call(b"response-token")]

    @pytest.mark.parametrize(
        "authenticate_header",
        [None, "", "Basic abc", "Negotiate", "Negotiate !!!"],
    )
    def test_rejects_missing_or_malformed_server_token(self, fake_spnego, authenticate_header):
        context = kerberos_module.KerberosAuthContext("chnode1.example.com")

        with pytest.raises(OperationalError, match="mutual authentication failed"):
            context.validate_response(authenticate_header)

    def test_rejects_incomplete_mutual_authentication(self, fake_spnego):
        fake_spnego.client.return_value.complete = False
        context = kerberos_module.KerberosAuthContext("chnode1.example.com")

        with pytest.raises(OperationalError, match="did not complete"):
            context.validate_response("Negotiate " + base64.b64encode(b"response-token").decode())

    def test_missing_pyspnego(self):
        with patch.object(options_module, "spnego", None, create=True):
            with pytest.raises(NotSupportedError):
                kerberos_module.KerberosAuthContext("host")

    def test_negotiation_failure_preserves_pyspnego_message(self):
        class _FakeSpnegoError(Exception):
            pass

        fake = MagicMock()
        fake.exceptions.SpnegoError = _FakeSpnegoError
        fake.client.return_value.step.side_effect = _FakeSpnegoError("credential cache is unavailable")

        with patch.object(options_module, "spnego", fake, create=True):
            with pytest.raises(OperationalError, match="credential cache is unavailable") as exc_info:
                kerberos_module.KerberosAuthContext("chnode1.example.com")

        assert exc_info.value.__cause__ is not None

    def test_missing_system_kerberos_support_is_operational_error(self):
        fake = MagicMock()
        fake.exceptions.SpnegoError = RuntimeError
        fake.client.side_effect = ImportError("GSSAPI support is unavailable")

        with patch.object(options_module, "spnego", fake, create=True):
            with pytest.raises(OperationalError, match="GSSAPI support is unavailable"):
                kerberos_module.KerberosAuthContext("chnode1.example.com")


def _response(status=200, authenticate_header="Negotiate response-token"):
    response = MagicMock()
    response.status = status
    response.headers = {"WWW-Authenticate": authenticate_header} if authenticate_header is not None else {}
    return response


def _kerberos_attempt(header):
    context = MagicMock()
    context.authorization_header = header
    return context


def _build_sync_kerberos_backend(hostname="chnode1.example.com"):
    return HttpSyncBackend(
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


class TestSyncKerberosRequest:
    def test_retry_uses_fresh_context_and_validates_success(self):
        backend = _build_sync_kerberos_backend()
        responses = iter([_response(503), _response()])
        request_kwargs = []

        def request(method, url, **kwargs):
            request_kwargs.append(dict(kwargs, headers=dict(kwargs["headers"])))
            return next(responses)

        backend.http.request.side_effect = request
        first_context = _kerberos_attempt("Negotiate request-1")
        second_context = _kerberos_attempt("Negotiate request-2")

        with patch(
            "clickhouse_connect.driver._backend.http_sync.KerberosAuthContext",
            side_effect=[first_context, second_context],
        ) as context_factory:
            response = backend.request(b"SELECT 13", {}, retries=1)

        assert response.status == 200
        assert [kwargs["headers"]["Authorization"] for kwargs in request_kwargs] == [
            "Negotiate request-1",
            "Negotiate request-2",
        ]
        assert [kwargs["retries"] for kwargs in request_kwargs] == [0, 0]
        assert context_factory.call_args_list == [call("chnode1.example.com"), call("chnode1.example.com")]
        first_context.validate_response.assert_not_called()
        second_context.validate_response.assert_called_once_with("Negotiate response-token")

    def test_success_without_server_token_fails(self):
        backend = _build_sync_kerberos_backend()
        backend.http.request.return_value = _response(authenticate_header=None)
        context = _kerberos_attempt("Negotiate request-token")
        context.validate_response.side_effect = OperationalError("missing server token")

        with patch("clickhouse_connect.driver._backend.http_sync.KerberosAuthContext", return_value=context):
            with pytest.raises(OperationalError, match="missing server token"):
                backend.request(b"SELECT 13", {})

        backend.http.request.return_value.close.assert_called_once()


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
        self._responses = iter(responses)
        self.closed = False
        self.headers = {}
        self.request_kwargs = []

    async def request(self, **kwargs):
        self.request_kwargs.append(dict(kwargs, headers=dict(kwargs["headers"])))
        return next(self._responses)


def _build_async_kerberos_backend(responses, hostname="chnode1.example.com", use_kerberos=True):
    backend = HttpAsyncBackend(
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
        use_kerberos=use_kerberos,
        kerberos_hostname=hostname if use_kerberos else None,
    )
    session = _FakeAsyncSession(responses)
    backend.session_lease = _FakeAsyncLease(session)
    return backend, session


class TestAsyncKerberosRequest:
    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("use_kerberos", "expected_method"),
        [(False, "GET"), (True, "POST")],
    )
    async def test_get_method_replay_protection(self, use_kerberos, expected_method):
        backend, session = _build_async_kerberos_backend([_response()], use_kerberos=use_kerberos)
        context = _kerberos_attempt("Negotiate request-token")

        with patch("clickhouse_connect.driver._backend.http_async.KerberosAuthContext", return_value=context):
            await backend.request(b"", {}, method="GET")

        assert session.request_kwargs[0]["method"] == expected_method
        assert ("allow_redirects" in session.request_kwargs[0]) is use_kerberos

    @pytest.mark.asyncio
    async def test_retry_uses_fresh_context_and_validates_success(self):
        backend, session = _build_async_kerberos_backend([_response(503), _response()])
        first_context = _kerberos_attempt("Negotiate request-1")
        second_context = _kerberos_attempt("Negotiate request-2")

        with patch(
            "clickhouse_connect.driver._backend.http_async.KerberosAuthContext",
            side_effect=[first_context, second_context],
        ) as context_factory:
            response = await backend.request(b"SELECT 13", {}, retries=1)

        assert response.status == 200
        assert [kwargs["headers"]["Authorization"] for kwargs in session.request_kwargs] == [
            "Negotiate request-1",
            "Negotiate request-2",
        ]
        assert all(kwargs["allow_redirects"] is False for kwargs in session.request_kwargs)
        assert context_factory.call_args_list == [call("chnode1.example.com"), call("chnode1.example.com")]
        first_context.validate_response.assert_not_called()
        second_context.validate_response.assert_called_once_with("Negotiate response-token")
