"""
Unit tests for the in-process chdb client backend.

These tests do not require a ClickHouse server — chdb is the embedded engine.
Skipped automatically if `chdb` is not installable (e.g. Windows or bare
install).
"""

from __future__ import annotations

import asyncio
from datetime import date, datetime
from decimal import Decimal

import pytest

chdb = pytest.importorskip("chdb")

import clickhouse_connect  # noqa: E402
from clickhouse_connect.driver.exceptions import (  # noqa: E402
    DatabaseError,
    NotSupportedError,
)


@pytest.fixture
def client():
    c = clickhouse_connect.get_client(interface="chdb")
    yield c
    c.close()


@pytest.fixture
def async_client():
    return clickhouse_connect.get_async_client


# ---- basic protocol ----


def test_ping(client):
    assert client.ping() is True


def test_server_version_populated(client):
    assert client.server_version
    assert client.server_version.split(".")[0].isdigit()


def test_uri_shape():
    c = clickhouse_connect.get_client(interface="chdb", chdb_path=":memory:")
    try:
        assert c.uri.startswith("chdb://")
    finally:
        c.close()


def test_chdb_connection_escape_hatch_exposed(client):
    assert client.chdb_connection is not None


# ---- query / command ----


def test_command_returns_scalar(client):
    assert client.command("SELECT 13") == 13
    assert client.command("SELECT 'user_1'") == "user_1"


def test_command_returns_tuple_for_multiple_columns(client):
    result = client.command("SELECT 79, 'user_2'")
    assert result == ["79", "user_2"]


def test_query_primitives(client):
    r = client.query(
        "SELECT toInt32(13) AS i, toString('user_1') AS s, toFloat64(3.14) AS f",
    )
    assert r.column_names == ("i", "s", "f")
    assert r.result_rows == [(13, "user_1", 3.14)]


def test_query_nullable_and_low_cardinality(client):
    r = client.query("SELECT CAST(NULL AS Nullable(Int64)) AS n, CAST('user_2' AS LowCardinality(String)) AS lc")
    row = r.result_rows[0]
    assert row[0] is None
    assert row[1] == "user_2"


def test_query_dates_decimals(client):
    r = client.query("SELECT toDate('2026-05-19') AS d, toDateTime('2026-05-19 10:30:00', 'UTC') AS dt, toDecimal64(123.456, 3) AS dec")
    d, dt, dec = r.result_rows[0]
    assert d == date(2026, 5, 19)
    assert dt == datetime(2026, 5, 19, 10, 30, 0)
    assert dec == Decimal("123.456")


def test_query_array_and_map(client):
    r = client.query("SELECT [1, 2, 3]::Array(UInt32) AS arr, map('user_1', 13, 'user_2', 79) AS m")
    arr, m = r.result_rows[0]
    assert list(arr) == [1, 2, 3]
    assert m == {"user_1": 13, "user_2": 79}


def test_query_multi_row(client):
    r = client.query("SELECT number FROM numbers(5)")
    assert [row[0] for row in r.result_rows] == [0, 1, 2, 3, 4]


def test_query_empty(client):
    r = client.query("SELECT 1 WHERE 0")
    assert r.result_rows == []


def test_raw_query_pass_through(client):
    body = client.raw_query("SELECT 13 AS x", fmt="TabSeparated")
    assert body == b"13\n"


# ---- insert paths ----


def test_insert_row_data(client):
    client.command("CREATE TABLE row_insert_test (id UInt32, name String) ENGINE = Memory")
    client.insert(
        "row_insert_test",
        [[13, "user_1"], [79, "user_2"]],
        column_names=["id", "name"],
    )
    r = client.query("SELECT id, name FROM row_insert_test ORDER BY id")
    assert r.result_rows == [(13, "user_1"), (79, "user_2")]


def test_insert_dataframe_fast_path(client):
    pd = pytest.importorskip("pandas")
    client.command("CREATE TABLE df_insert_test (id UInt32, v Float64) ENGINE = Memory")
    df = pd.DataFrame({"id": [13, 79, 103], "v": [1.5, 2.5, 3.5]})
    client.insert_df("df_insert_test", df)
    r = client.query("SELECT id, v FROM df_insert_test ORDER BY id")
    assert r.result_rows == [(13, 1.5), (79, 2.5), (103, 3.5)]


def test_insert_dataframe_reordered_columns(client):
    pd = pytest.importorskip("pandas")
    client.command("CREATE TABLE df_reorder (id UInt32, v Float64) ENGINE = Memory")
    df = pd.DataFrame({"v": [9.5, 10.5], "id": [13, 79]})  # reversed
    client.insert_df("df_reorder", df)
    r = client.query("SELECT id, v FROM df_reorder ORDER BY id")
    assert r.result_rows == [(13, 9.5), (79, 10.5)]


def test_raw_insert_bytes_round_trip(client):
    client.command("CREATE TABLE raw_insert_test (id UInt32, v String) ENGINE = Memory")
    csv = b"13,user_1\n79,user_2\n"
    client.raw_insert("raw_insert_test", insert_block=csv, fmt="CSV")
    r = client.query("SELECT id, v FROM raw_insert_test ORDER BY id")
    assert r.result_rows == [(13, "user_1"), (79, "user_2")]


# ---- session semantics ----


def test_session_persistence_within_client(client):
    client.command("CREATE TEMPORARY TABLE temp_persist (id Int32)")
    client.command("INSERT INTO temp_persist VALUES (13), (79)")
    r = client.query("SELECT count() FROM temp_persist")
    assert r.result_rows[0][0] == 2


def test_set_client_setting_persists(client):
    client.set_client_setting("max_block_size", 1000)
    assert client.get_client_setting("max_block_size") == "1000"


# ---- streaming ----


def test_query_row_block_stream(client):
    with client.query_row_block_stream("SELECT number FROM numbers(50) SETTINGS max_block_size = 10") as stream:
        blocks = list(stream)
    assert sum(len(b) for b in blocks) == 50


def test_raw_stream_iterates(client):
    stream = client.raw_stream("SELECT number FROM numbers(5)", fmt="CSV")
    try:
        data = stream.read()
    finally:
        stream.close()
    assert data.startswith(b"0\n")


# ---- error mapping ----


def test_unknown_function_maps_to_database_error(client):
    with pytest.raises(DatabaseError) as ex_info:
        client.query("SELECT bad_function()")
    assert "UNKNOWN_FUNCTION" in str(ex_info.value) or "bad_function" in str(ex_info.value)


def test_external_data_not_supported(client):
    from clickhouse_connect.driver.external import ExternalData

    ext = ExternalData(file_name="x.csv", data=b"1\n2\n", fmt="CSV", structure="id UInt32")
    with pytest.raises(NotSupportedError):
        client.query("SELECT * FROM x", external_data=ext)


# ---- HTTP-only kwargs accepted silently ----


def test_http_only_kwargs_silently_ignored():
    c = clickhouse_connect.get_client(
        interface="chdb",
        username="default",
        password="ignored",
        compress=True,
        connect_timeout=10,
        verify=True,
        http_proxy="http://localhost:3128",
    )
    try:
        assert c.ping() is True
    finally:
        c.close()


def test_set_access_token_silent_noop(client):
    client.set_access_token("not-a-real-token")  # must not raise


# ---- DBAPI on top of chdb ----


def test_dbapi_cursor_round_trip():
    import clickhouse_connect.dbapi as dbapi

    conn = dbapi.connect(interface="chdb")
    try:
        cur = conn.cursor()
        try:
            cur.execute("CREATE TABLE dba_round_trip (id UInt32, name String) ENGINE = Memory")
            cur.execute("INSERT INTO dba_round_trip VALUES (13, 'user_1'), (79, 'user_2')")
            cur.execute("SELECT id, name FROM dba_round_trip ORDER BY id")
            rows = cur.fetchall()
            assert rows == [(13, "user_1"), (79, "user_2")]
            assert [c[0] for c in cur.description] == ["id", "name"]
        finally:
            cur.close()
    finally:
        conn.close()


# ---- async client ----


def test_async_client_basic_flow():
    async def run():
        c = await clickhouse_connect.get_async_client(interface="chdb")
        try:
            assert await c.ping() is True
            r = await c.query("SELECT 13 AS x")
            assert r.result_rows == [(13,)]
            await c.command("CREATE TABLE async_smoke (id UInt32) ENGINE = Memory")
            await c.insert("async_smoke", [[13], [79]], column_names=["id"])
            r = await c.query("SELECT count() FROM async_smoke")
            assert r.result_rows[0][0] == 2
        finally:
            await c.close()

    asyncio.run(run())


def test_async_client_gather_serializes_without_error():
    async def run():
        c = await clickhouse_connect.get_async_client(interface="chdb")
        try:
            results = await asyncio.gather(
                c.query("SELECT 13"),
                c.query("SELECT 79"),
                c.query("SELECT 103"),
            )
            values = [r.result_rows[0][0] for r in results]
            assert sorted(values) == [13, 79, 103]
        finally:
            await c.close()

    asyncio.run(run())


def test_async_dataframe_fast_path():
    pd = pytest.importorskip("pandas")

    async def run():
        c = await clickhouse_connect.get_async_client(interface="chdb")
        try:
            await c.command("CREATE TABLE async_df (id UInt32, v Float64) ENGINE = Memory")
            df = pd.DataFrame({"id": [13, 79], "v": [1.5, 2.5]})
            await c.insert_df("async_df", df)
            out = await c.query_df("SELECT id, v FROM async_df ORDER BY id")
            assert list(out["id"]) == [13, 79]
            assert list(out["v"]) == [1.5, 2.5]
        finally:
            await c.close()

    asyncio.run(run())


# ---- factory / dispatch ----


def test_factory_dispatches_on_interface():
    c = clickhouse_connect.get_client(interface="chdb")
    try:
        from clickhouse_connect.driver.chdbclient import ChdbClient

        assert isinstance(c, ChdbClient)
    finally:
        c.close()
