import pytest

from clickhouse_connect.driver import httputil


@pytest.mark.parametrize("dedicated_pool", [False, True])
def test_dedicated_pool_option(client_factory, call, dedicated_pool):
    first = client_factory(dedicated_pool=dedicated_pool)
    second = client_factory(dedicated_pool=dedicated_pool)
    assert call(first.command, "SELECT 13") == 13
    call(first.close)
    call(first.close)
    assert call(second.command, "SELECT 79") == 79


def test_owned_pool_closes_idle_connections(client_factory, client_mode):
    if client_mode != "sync":
        pytest.skip("urllib3 pool lifecycle is specific to the synchronous transport")
    client = client_factory(dedicated_pool=True, compress=False)
    assert client.command("SELECT 13") == 13
    manager = client.http
    with manager.pools.lock:
        (pool,) = [manager.pools[key] for key in manager.pools.keys()]
    connection = pool._get_conn()
    assert connection.sock is not None
    pool._put_conn(connection)

    client.close()

    assert connection.sock is None
    assert pool.pool is None
    assert manager not in httputil.all_managers


def test_owned_pool_stream_finishes_after_close(client_factory, client_mode):
    if client_mode != "sync":
        pytest.skip("urllib3 pool lifecycle is specific to the synchronous transport")
    client = client_factory(dedicated_pool=True, compress=False)
    with client.raw_stream("SELECT number FROM numbers(100000)", fmt="TabSeparated") as response:
        connection = response._connection
        pool = response._pool
        assert connection is not None
        assert connection.sock is not None
        prefix = response.read(13)
        assert response._connection is connection

        client.close()

        assert pool.pool is None
        assert connection.sock is not None
        body = prefix + response.read()
        assert body.startswith(b"0\n1\n2\n")
        assert body.endswith(b"99999\n")
        assert body.count(b"\n") == 100000
        assert connection.sock is None
