import os
from unittest.mock import patch

import aiohttp
import pytest
from urllib3.poolmanager import PoolManager

from clickhouse_connect.driver.common import coerce_bool
from clickhouse_connect.driver.exceptions import DatabaseError
from clickhouse_connect.driver.options import spnego
from tests.integration_tests import kerberos_manage


def _kerberos_enabled():
    return coerce_bool(os.environ.get("CLICKHOUSE_CONNECT_TEST_KERBEROS", "False"))


# All tests here must land on the same xdist worker (see --dist=loadgroup in pyproject.toml):
# kerberos_env below does real docker compose/kinit side effects that must run exactly once, not
# once per worker.
pytestmark = [
    pytest.mark.xdist_group(name="kerberos"),
    pytest.mark.skipif(not _kerberos_enabled(), reason="CLICKHOUSE_CONNECT_TEST_KERBEROS is False"),
    pytest.mark.skipif(spnego is None, reason="kerberos option not installed"),
]


@pytest.fixture(scope="module", autouse=True)
def kerberos_env():
    kerberos_manage.setup()
    yield
    kerberos_manage.teardown()


def test_basic_kerberos_auth(client_factory, call):
    client = client_factory(
        host=kerberos_manage.CLICKHOUSE_HOST,
        port=kerberos_manage.CLICKHOUSE_PORT,
        username="",
        password="",
        database="default",
        use_kerberos=True,
    )
    assert call(client.command, "SELECT currentUser()") == "kuser"


def test_kerberos_hostname_override(client_factory, call):
    client = client_factory(
        host="localhost",
        port=kerberos_manage.CLICKHOUSE_PORT,
        username="",
        password="",
        database="default",
        use_kerberos=True,
        kerberos_hostname_override=kerberos_manage.CLICKHOUSE_HOST,
    )
    assert call(client.command, "SELECT currentUser()") == "kuser"


def test_kerberos_insert_and_query(client_factory, call):
    client = client_factory(
        host=kerberos_manage.CLICKHOUSE_HOST,
        port=kerberos_manage.CLICKHOUSE_PORT,
        username="",
        password="",
        database="default",
        use_kerberos=True,
    )
    call(client.command, "CREATE TABLE IF NOT EXISTS default.krb_int_test (id UInt32, name String) ENGINE = Memory")
    try:
        call(client.insert, "default.krb_int_test", [[13, "alpha"], [79, "beta"]], column_names=["id", "name"])
        result = call(client.query, "SELECT id, name FROM default.krb_int_test ORDER BY id")
        assert result.result_rows == [(13, "alpha"), (79, "beta")]
    finally:
        call(client.command, "DROP TABLE IF EXISTS default.krb_int_test")


def _patch_auth_headers(on_header):
    """Patch the real sync/async transport calls to observe (and optionally rewrite, via
    in-place mutation) the Authorization header of each outgoing request. Returns the two patch
    context managers to enter together; the real request always goes through underneath."""
    real_sync_request = PoolManager.request
    real_async_request = aiohttp.ClientSession.request

    def sync_wrapper(self, method, url, *args, **kwargs):
        headers = dict(kwargs.get("headers") or {})
        on_header(headers)
        return real_sync_request(self, method, url, *args, **{**kwargs, "headers": headers})

    async def async_wrapper(self, method, url, *args, **kwargs):
        headers = dict(kwargs.get("headers") or {})
        on_header(headers)
        return await real_async_request(self, method, url, *args, **{**kwargs, "headers": headers})

    return patch.object(PoolManager, "request", new=sync_wrapper), patch.object(aiohttp.ClientSession, "request", new=async_wrapper)


def test_kerberos_multiple_sequential_requests(client_factory, call):
    # ClickHouse authenticates each HTTP request independently (no session carryover), so a
    # fresh Negotiate token must be generated and accepted on every single request, not just
    # the first one on a connection. Headers are captured at the actual transport call
    # (urllib3/aiohttp), not from negotiate_auth_header's return value, so this confirms what was
    # truly sent over the wire rather than just what the header builder produced.
    client = client_factory(
        host=kerberos_manage.CLICKHOUSE_HOST,
        port=kerberos_manage.CLICKHOUSE_PORT,
        username="",
        password="",
        database="default",
        use_kerberos=True,
    )
    headers_sent = []

    def _record(headers):
        auth = headers.get("Authorization")
        if auth:
            headers_sent.append(auth)

    sync_patch, async_patch = _patch_auth_headers(_record)
    with sync_patch, async_patch:
        for _ in range(3):
            assert call(client.command, "SELECT currentUser()") == "kuser"

    assert len(headers_sent) == 3
    assert len(set(headers_sent)) == 3, "expected a fresh Negotiate header per request, got a reused one"


def test_kerberos_rejects_reused_header(client_factory, call):
    # Complements test_kerberos_multiple_sequential_requests, proving the guarantee from the
    # other direction: ClickHouse actually rejects a stale/reused Negotiate header rather than
    # merely happening to receive a fresh one each time.
    client = client_factory(
        host=kerberos_manage.CLICKHOUSE_HOST,
        port=kerberos_manage.CLICKHOUSE_PORT,
        username="",
        password="",
        database="default",
        use_kerberos=True,
    )
    captured = {"value": None}

    def _force_reuse(headers):
        if captured["value"] is None:
            captured["value"] = headers.get("Authorization")
        else:
            headers["Authorization"] = captured["value"]

    sync_patch, async_patch = _patch_auth_headers(_force_reuse)
    with sync_patch, async_patch:
        assert call(client.command, "SELECT currentUser()") == "kuser"
        with pytest.raises(DatabaseError):
            call(client.command, "SELECT currentUser()")
