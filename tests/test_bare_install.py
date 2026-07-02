import clickhouse_connect


def test_bare_install():
    """Bare install test to validate the package works with only core dependencies"""
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
