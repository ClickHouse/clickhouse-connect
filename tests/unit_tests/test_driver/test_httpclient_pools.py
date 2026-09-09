import threading
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import Mock

import pytest
from urllib3.poolmanager import PoolManager

from clickhouse_connect.driver import httpclient, httputil
from clickhouse_connect.driver.client import Client
from clickhouse_connect.driver.exceptions import OperationalError, ProgrammingError
from clickhouse_connect.driver.httpclient import HttpClient


def make_client(**kwargs):
    options = {
        "interface": "https",
        "host": "localhost",
        "port": 8443,
        "username": "default",
        "password": "",
        "database": "default",
        "server_host_name": "localhost",
        "native_codec": "python",
    }
    options.update(kwargs)
    return HttpClient(**options)


@pytest.fixture
def pools(monkeypatch):
    managers = []
    manager_lock = threading.Lock()
    get_pool_manager = httputil.get_pool_manager

    def track_manager(**kwargs):
        manager = get_pool_manager(**kwargs)
        manager.connection_from_host("localhost", 8443, scheme="https")
        manager.clear = Mock(wraps=manager.clear)
        with manager_lock:
            managers.append(manager)
        return manager

    monkeypatch.setattr(httputil, "get_pool_manager", track_manager)
    monkeypatch.setattr(httpclient, "get_pool_manager", track_manager)
    monkeypatch.setattr(httpclient, "check_env_proxy", Mock(return_value=None))
    monkeypatch.setattr(httputil, "_proxy_managers", {})
    monkeypatch.setattr(Client, "_init_common_settings", Mock())
    yield managers
    for manager in managers:
        PoolManager.clear(manager)
        httputil.all_managers.pop(manager, None)


@pytest.mark.parametrize(
    ("options", "error_type"),
    [
        ({"connect_timeout": "invalid"}, ValueError),
        ({"send_receive_timeout": "invalid"}, ValueError),
        ({"connect_timeout": 0}, ValueError),
        ({"native_codec": "invalid"}, ProgrammingError),
        ({"compress": "invalid"}, ProgrammingError),
        ({"query_limit": "invalid"}, ValueError),
        ({"tz_mode": "invalid"}, ProgrammingError),
        ({"settings": {"query": "SELECT 13"}}, ProgrammingError),
    ],
)
def test_invalid_configuration_releases_owned_pool(pools, options, error_type):
    with pytest.raises(error_type):
        make_client(**options)

    (manager,) = pools
    manager.clear.assert_called_once_with()
    assert len(manager.pools) == 0
    assert manager not in httputil.all_managers


@pytest.mark.parametrize("stage", ["token", "backend", "initialization"])
@pytest.mark.parametrize("error_type", [OperationalError, KeyboardInterrupt])
def test_constructor_error_releases_owned_pool(pools, monkeypatch, stage, error_type):
    error = error_type("construction failed")
    fail = Mock(side_effect=error)
    options = {}
    if stage == "token":
        options["token_provider"] = fail
    elif stage == "backend":
        monkeypatch.setattr(httpclient, "HttpSyncBackend", fail)
    else:
        monkeypatch.setattr(Client, "_init_common_settings", fail)

    with pytest.raises(error_type) as caught:
        make_client(**options)

    assert caught.value is error
    (manager,) = pools
    manager.clear.assert_called_once_with()
    assert len(manager.pools) == 0
    assert manager not in httputil.all_managers


@pytest.mark.parametrize(
    ("cleanup_error_type", "raised_type"),
    [(RuntimeError, OperationalError), (KeyboardInterrupt, KeyboardInterrupt)],
)
@pytest.mark.parametrize("stage", ["token", "initialization"])
def test_cleanup_failure_preserves_construction_error(pools, monkeypatch, cleanup_error_type, raised_type, stage):
    error = OperationalError("construction failed")

    def fail(*_args):
        pools[0].clear.side_effect = cleanup_error_type("cleanup failed")
        raise error

    options = {"token_provider": fail} if stage == "token" else {}
    if stage == "initialization":
        monkeypatch.setattr(Client, "_init_common_settings", fail)

    with pytest.raises(raised_type) as caught:
        make_client(**options)

    if raised_type is OperationalError:
        assert caught.value is error
    else:
        # An interrupt during cleanup propagates and keeps the construction error as context.
        assert caught.value.__context__ is error
    pools[0].clear.assert_called_once_with()
    assert pools[0] not in httputil.all_managers


@pytest.mark.parametrize("pool_kind", ["supplied", "default_http", "default_https", "proxy"])
@pytest.mark.parametrize("fail_construction", [False, True])
def test_borrowed_pool_stays_open(pools, monkeypatch, pool_kind, fail_construction):
    manager = httputil.get_pool_manager()
    options = {}
    if pool_kind == "supplied":
        options["pool_mgr"] = manager
    elif pool_kind == "proxy":
        options.update(interface="http", http_proxy="http://proxy.test:8080")
        httputil._proxy_managers["localhost__http://proxy.test:8080"] = manager
    else:
        monkeypatch.setattr(httputil, "_default_pool_manager", manager)
        options.update(interface="http" if pool_kind == "default_http" else "https", server_host_name=None)

    if fail_construction:
        with pytest.raises(ProgrammingError, match="tz_mode"):
            make_client(**options, tz_mode="invalid")
    else:
        client = make_client(**options)
        assert client.http is manager
        client.close()

    assert pools == [manager]
    manager.clear.assert_not_called()
    assert len(manager.pools) == 1
    assert manager in httputil.all_managers


@pytest.mark.parametrize(
    "options",
    [
        {"server_host_name": "localhost"},
        {"ca_cert": "test-ca.pem"},
        {"client_cert": "test-client.pem"},
        {"verify": False},
        {"https_proxy": "http://proxy.test:8080"},
    ],
)
def test_successful_client_releases_owned_pool_on_close(pools, options):
    client = make_client(**{"server_host_name": None, **options})
    (manager,) = pools
    assert client.http is manager
    assert manager in httputil.all_managers
    manager.clear.assert_not_called()

    client.close()

    manager.clear.assert_called_once_with()
    assert len(manager.pools) == 0
    assert manager not in httputil.all_managers


def test_close_failure_unregisters_owned_pool(pools):
    client = make_client()
    (manager,) = pools
    error = RuntimeError("cleanup failed")
    manager.clear.side_effect = error

    with pytest.raises(RuntimeError) as caught:
        client.close()

    assert caught.value is error
    manager.clear.assert_called_once_with()
    assert manager not in httputil.all_managers


def test_concurrent_failure_preserves_new_successful_client_pool(pools):
    allocated = threading.Event()
    release = threading.Event()
    error = OperationalError("construction failed")

    def fail_token():
        allocated.set()
        assert release.wait(timeout=5)
        raise error

    client = None
    try:
        with ThreadPoolExecutor(max_workers=1) as executor:
            failed_client = executor.submit(make_client, token_provider=fail_token)
            try:
                assert allocated.wait(timeout=5)
                client = make_client()
            finally:
                release.set()
            with pytest.raises(OperationalError) as caught:
                failed_client.result(timeout=5)

        assert caught.value is error
        failed_manager, live_manager = pools
        failed_manager.clear.assert_called_once_with()
        assert failed_manager not in httputil.all_managers
        assert client.http is live_manager
        live_manager.clear.assert_not_called()
        assert len(live_manager.pools) == 1
        assert live_manager in httputil.all_managers
    finally:
        if client is not None:
            client.close()
