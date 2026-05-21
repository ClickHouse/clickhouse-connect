import importlib.util

import clickhouse_connect


def test_chdb_backend_missing_dep_raises_clean_error():
    """Without chdb installed, requesting interface='chdb' must surface a clean ImportError.

    The bare install CI job deliberately omits the chdb extra, so this verifies the friendly
    error path. If chdb happens to be importable (local dev), this assertion is skipped.
    """
    if importlib.util.find_spec("chdb") is not None:
        print("chdb is installed; skipping missing-dep error path check")
        return
    try:
        clickhouse_connect.get_client(interface="chdb")
    except ImportError as ex:
        assert "chdb" in str(ex), f"expected chdb in error message, got: {ex}"
        return
    raise AssertionError("Expected ImportError when chdb is not installed")


def test_bare_install():
    """Bare install test to validate the package works with only core dependencies"""
    test_chdb_backend_missing_dep_raises_clean_error()

    client = clickhouse_connect.get_client()

    ver = client.command("SELECT version()")
    assert isinstance(ver, str) and len(ver) > 0, f"unexpected version: {ver}"

    result = client.query("SELECT 1 AS x, 2 AS y")
    assert result.result_rows == [(1, 2)], f"unexpected result: {result.result_rows}"

    client.command("DROP TABLE IF EXISTS _bare_install_test")
    client.command("CREATE TABLE _bare_install_test (id UInt32, val String) ENGINE MergeTree ORDER BY id")
    client.insert("_bare_install_test", [[1, "a"], [2, "b"]], column_names=["id", "val"])
    res = client.query("SELECT * FROM _bare_install_test ORDER BY id")
    assert res.result_rows == [(1, "a"), (2, "b")], f"unexpected: {res.result_rows}"
    client.command("DROP TABLE _bare_install_test")


if __name__ == "__main__":
    test_chdb_backend_missing_dep_raises_clean_error()
    test_bare_install()
