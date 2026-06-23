"""
Backend protocol contract tests.

These assert the *generic* guarantees clickhouse-connect makes about the pluggable backend
machinery, with no dependency on any specific backend package. An out-of-tree backend repo
runs an extended version of these against its own ``Backend`` implementation; here we cover
the registry, capability-flag defaults, error routing, and the ``backend=`` plumbing using a
minimal in-test backend.
"""

from __future__ import annotations

from typing import Any

import pytest

import clickhouse_connect
from clickhouse_connect.driver.backend import SUPPORTS_ZERO_COPY_ARROW, Backend, client_supports
from clickhouse_connect.driver.exceptions import BackendNotInstalledError, DatabaseError
from clickhouse_connect.driver.registry import available_backend_names, resolve_backend


class _StubClient:
    """Stand-in for a Client carrying just the attributes the capability API reads."""

    def __init__(self, zero_copy: bool):
        self.backend_name = "stub"
        if zero_copy:
            setattr(self, SUPPORTS_ZERO_COPY_ARROW, True)

    def map_error(self, exc: BaseException) -> Exception:
        return DatabaseError(str(exc))


class _StubBackend:
    backend_name = "stub"

    def create_client(self, **kwargs: Any) -> _StubClient:
        return _StubClient(zero_copy=bool(kwargs.get("zero_copy", False)))

    def create_async_client(self, **kwargs: Any) -> _StubClient:
        return _StubClient(zero_copy=bool(kwargs.get("zero_copy", False)))


def test_http_backend_always_available():
    assert "http" in available_backend_names()


def test_resolve_http_raises_programming_error():
    # "http" is the built-in transport, not a registered backend; resolve_backend("http") must
    # reject it explicitly so the inconsistency between available_backend_names() (which always
    # includes "http") and registry resolution is loud and clear.
    from clickhouse_connect.driver.exceptions import ProgrammingError

    with pytest.raises(ProgrammingError) as exc_info:
        resolve_backend("http")
    msg = str(exc_info.value)
    assert "http" in msg and "built-in" in msg


def test_resolve_unknown_backend_raises_with_hint():
    with pytest.raises(BackendNotInstalledError) as exc_info:
        resolve_backend("definitely_not_a_real_backend")
    err = exc_info.value
    assert err.backend_name == "definitely_not_a_real_backend"
    assert "http" in err.available


def test_resolve_chdb_hint_present_when_not_installed():
    # When chdb is not installed the error must still name the documented install command.
    try:
        resolve_backend("chdb")
    except BackendNotInstalledError as err:
        assert err.hint == "pip install clickhouse-connect[chdb]"


def test_get_client_unknown_backend_raises():
    with pytest.raises(BackendNotInstalledError):
        clickhouse_connect.get_client(backend="definitely_not_a_real_backend")


def test_capability_flag_defaults_false():
    client = _StubClient(zero_copy=False)
    assert client_supports(client, SUPPORTS_ZERO_COPY_ARROW) is False


def test_capability_flag_reads_true():
    client = _StubClient(zero_copy=True)
    assert client_supports(client, SUPPORTS_ZERO_COPY_ARROW) is True


def test_stub_backend_satisfies_protocol():
    # runtime_checkable Protocol: a conforming object must be recognized as a Backend.
    assert isinstance(_StubBackend(), Backend)


def test_backend_factory_builds_client():
    client = _StubBackend().create_client(zero_copy=True)
    assert client.backend_name == "stub"
    assert client_supports(client, SUPPORTS_ZERO_COPY_ARROW) is True


def test_error_mapper_returns_cc_exception():
    client = _StubClient(zero_copy=False)
    mapped = client.map_error(RuntimeError("boom"))
    from clickhouse_connect.driver.exceptions import ClickHouseError

    assert isinstance(mapped, ClickHouseError)


def test_default_backend_is_http_byte_identical(monkeypatch):
    # Passing backend="http" (or omitting it) must not divert to the registry path.
    # We only verify the routing decision -- whether the resulting HttpClient construction
    # then succeeds (or fails because port 1 is closed) is unrelated to the routing check.
    called = {"resolve": False}
    import clickhouse_connect.driver.registry as registry

    def _fail(_name):
        called["resolve"] = True
        raise AssertionError("registry must not be consulted for the http backend")

    monkeypatch.setattr(registry, "resolve_backend", _fail)
    try:
        client = clickhouse_connect.get_client(backend="http", host="127.0.0.1", port=1)
        client.close()
    except Exception:  # noqa: BLE001 -- connection failure is fine; the routing check is what matters
        pass
    assert called["resolve"] is False
