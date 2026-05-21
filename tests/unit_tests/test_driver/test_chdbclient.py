"""
Unit tests for the in-process chdb client backend.

These tests do not require a ClickHouse server — chdb is the embedded engine.
Skipped automatically if `chdb` is not installable (e.g. Windows or bare
install).
"""

from __future__ import annotations

import asyncio
import io
import os
from datetime import date, datetime
from decimal import Decimal
from uuid import UUID

import pytest

chdb = pytest.importorskip("chdb")

import clickhouse_connect  # noqa: E402
from clickhouse_connect.driver.chdbclient import _build_conn_string, _format_error_message  # noqa: E402
from clickhouse_connect.driver.exceptions import (  # noqa: E402
    DatabaseError,
    NotSupportedError,
    ProgrammingError,
)


@pytest.fixture
def client():
    c = clickhouse_connect.get_client(interface="chdb")
    yield c
    c.close()


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


def test_insert_dataframe(client):
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


def _read_session_setting(client, name: str) -> str:
    body = client.raw_query(f"SELECT value FROM system.settings WHERE name = '{name}'", fmt="TabSeparated")
    return body.decode().strip()


def test_command_per_call_setting_does_not_leak(client):
    before = _read_session_setting(client, "max_block_size")
    client.command("SELECT 1", settings={"max_block_size": 13})
    after = _read_session_setting(client, "max_block_size")
    assert after == before, f"max_block_size leaked: before={before!r} after={after!r}"


def test_command_per_call_setting_restored_on_error(client):
    before = _read_session_setting(client, "max_block_size")
    with pytest.raises(DatabaseError):
        client.command("SELECT bad_function()", settings={"max_block_size": 13})
    after = _read_session_setting(client, "max_block_size")
    assert after == before, f"max_block_size leaked after error: before={before!r} after={after!r}"


def test_command_restores_previously_set_value(client):
    client.set_client_setting("max_block_size", 7)
    client.command("SELECT 1", settings={"max_block_size": 13})
    assert _read_session_setting(client, "max_block_size") == "7"


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


def test_async_dataframe_insert():
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


# ---- pure helper unit tests (no chdb instance needed) ----


def test_build_conn_string_default_memory():
    assert _build_conn_string("", None) == ":memory:"
    assert _build_conn_string(None, None) == ":memory:"  # type: ignore[arg-type]


def test_build_conn_string_path_unchanged_without_options():
    assert _build_conn_string("/data/db", None) == "/data/db"
    assert _build_conn_string("file:/data/db?mode=ro", None) == "file:/data/db?mode=ro"


def test_build_conn_string_appends_options():
    assert _build_conn_string("/data/db", {"mode": "ro"}) == "/data/db?mode=ro"


def test_build_conn_string_merges_with_existing_query():
    result = _build_conn_string("file:/data/db?already=set", {"max_threads": 4})
    assert "already=set" in result and "max_threads=4" in result and "&" in result


def test_format_error_message_extracts_code_prefix():
    raw = "Some prefix\nCode: 46. DB::Exception: Function with name `bad` does not exist."
    assert _format_error_message(raw).startswith("Code: 46.")


def test_format_error_message_passes_through_plain_text():
    assert _format_error_message("plain error") == "plain error"
    assert _format_error_message("") == ""


# ---- closed client and lifecycle ----


def test_query_after_close_raises():
    c = clickhouse_connect.get_client(interface="chdb")
    c.close()
    with pytest.raises(ProgrammingError):
        c.query("SELECT 1")


def test_close_is_idempotent():
    c = clickhouse_connect.get_client(interface="chdb")
    c.close()
    c.close()  # must not raise


def test_close_connections_closes_client():
    c = clickhouse_connect.get_client(interface="chdb")
    c.close_connections()
    with pytest.raises(ProgrammingError):
        c.query("SELECT 1")


def test_context_manager_closes_client():
    with clickhouse_connect.get_client(interface="chdb") as c:
        assert c.ping() is True
    with pytest.raises(ProgrammingError):
        c.query("SELECT 1")


# ---- chdb_path persistence ----


def test_chdb_path_persists_across_clients(tmp_path):
    db_path = str(tmp_path / "persisted.db")

    a = clickhouse_connect.get_client(interface="chdb", chdb_path=db_path)
    try:
        a.command("CREATE TABLE persisted (id UInt32) ENGINE = MergeTree ORDER BY id")
        a.insert("persisted", [[13], [79]], column_names=["id"])
    finally:
        a.close()

    b = clickhouse_connect.get_client(interface="chdb", chdb_path=db_path)
    try:
        rows = b.query("SELECT id FROM persisted ORDER BY id").result_rows
        assert rows == [(13,), (79,)]
    finally:
        b.close()


# ---- per-call settings on query / insert ----


def test_per_call_settings_appended_to_select(client):
    # Setting that affects output rather than just performance, so we can verify it
    # actually reached chdb. `output_format_decimal_trailing_zeros` controls Decimal
    # text formatting, but for verification we use a behavior we can observe.
    r = client.query("SELECT number FROM numbers(10)", settings={"max_block_size": 3})
    assert [row[0] for row in r.result_rows] == [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]


def test_per_call_settings_do_not_leak_via_query(client):
    before = _read_session_setting(client, "max_block_size")
    client.query("SELECT 1", settings={"max_block_size": 17})
    after = _read_session_setting(client, "max_block_size")
    # query path uses inline SETTINGS clause (not SET), so it should never modify
    # the session value at all.
    assert after == before


# ---- show_clickhouse_errors ----


def test_show_clickhouse_errors_false_sanitizes_message():
    c = clickhouse_connect.get_client(interface="chdb", show_clickhouse_errors=False)
    try:
        with pytest.raises(DatabaseError) as ex_info:
            c.query("SELECT bad_function()")
        assert "UNKNOWN_FUNCTION" not in str(ex_info.value)
        assert "bad_function" not in str(ex_info.value)
    finally:
        c.close()


# ---- query_limit ----


def test_query_limit_auto_appends_limit():
    c = clickhouse_connect.get_client(interface="chdb", query_limit=3)
    try:
        rows = c.query("SELECT number FROM numbers(100)").result_rows
        assert len(rows) == 3
    finally:
        c.close()


def test_explicit_limit_not_overridden_by_query_limit():
    c = clickhouse_connect.get_client(interface="chdb", query_limit=3)
    try:
        rows = c.query("SELECT number FROM numbers(100) LIMIT 7").result_rows
        assert len(rows) == 7
    finally:
        c.close()


# ---- streaming variations ----


def test_raw_stream_via_context_manager(client):
    with client.raw_stream("SELECT number FROM numbers(5)", fmt="CSV") as stream:
        data = stream.read()
    assert data == b"0\n1\n2\n3\n4\n"


def test_raw_stream_chunked_read(client):
    stream = client.raw_stream("SELECT number FROM numbers(50)", fmt="CSV")
    try:
        out = b""
        while chunk := stream.read(8):
            out += chunk
    finally:
        stream.close()
    assert out == b"".join(f"{n}\n".encode() for n in range(50))


def test_raw_stream_readinto(client):
    stream = client.raw_stream("SELECT number FROM numbers(3)", fmt="CSV")
    try:
        buf = bytearray(64)
        n = stream.readinto(buf)
        assert buf[:n] == b"0\n1\n2\n"
    finally:
        stream.close()


def test_stream_release_lock_on_close(client):
    # If close() doesn't release the lock, the next query would deadlock.
    stream = client.raw_stream("SELECT 1", fmt="CSV")
    stream.close()
    # Should return immediately, no deadlock:
    assert client.query("SELECT 1").result_rows == [(1,)]


# ---- raw_insert input shapes ----


def test_raw_insert_accepts_str(client):
    client.command("CREATE TABLE raw_str (id UInt32, v String) ENGINE = Memory")
    client.raw_insert("raw_str", insert_block="13,user_1\n79,user_2\n", fmt="CSV")
    r = client.query("SELECT id, v FROM raw_str ORDER BY id")
    assert r.result_rows == [(13, "user_1"), (79, "user_2")]


def test_raw_insert_accepts_file_like(client):
    client.command("CREATE TABLE raw_file (id UInt32, v String) ENGINE = Memory")
    buf = io.BytesIO(b"13,user_1\n79,user_2\n")
    client.raw_insert("raw_file", insert_block=buf, fmt="CSV")
    r = client.query("SELECT id, v FROM raw_file ORDER BY id")
    assert r.result_rows == [(13, "user_1"), (79, "user_2")]


def test_raw_insert_accepts_generator(client):
    client.command("CREATE TABLE raw_gen (id UInt32, v String) ENGINE = Memory")

    def chunks():
        yield b"13,user_1\n"
        yield b"79,user_2\n"

    client.raw_insert("raw_gen", insert_block=chunks(), fmt="CSV")
    r = client.query("SELECT id, v FROM raw_gen ORDER BY id")
    assert r.result_rows == [(13, "user_1"), (79, "user_2")]


def test_raw_insert_compression_rejected(client):
    client.command("CREATE TABLE raw_compress (id UInt32) ENGINE = Memory")
    with pytest.raises(NotSupportedError):
        client.raw_insert("raw_compress", insert_block=b"1\n", fmt="CSV", compression="lz4")


def test_raw_insert_missing_args(client):
    with pytest.raises(ProgrammingError):
        client.raw_insert(None, insert_block=b"x")  # type: ignore[arg-type]
    with pytest.raises(ProgrammingError):
        client.raw_insert("t", insert_block=None)


def test_raw_insert_cleans_up_temp_file(client, monkeypatch):
    """Verify the temp file is deleted even when chdb errors."""
    client.command("CREATE TABLE raw_cleanup (id UInt32) ENGINE = Memory")
    seen_paths = []

    import tempfile as _tempfile

    original = _tempfile.NamedTemporaryFile

    def tracking(*args, **kwargs):
        f = original(*args, **kwargs)
        seen_paths.append(f.name)
        return f

    monkeypatch.setattr(_tempfile, "NamedTemporaryFile", tracking)

    # Bad CSV content for an UInt32 column will cause chdb to error.
    with pytest.raises(DatabaseError):
        client.raw_insert("raw_cleanup", insert_block=b"not_a_number\n", fmt="CSV")

    assert seen_paths, "temp file path not captured"
    for p in seen_paths:
        assert not os.path.exists(p), f"temp file leaked: {p}"


# ---- additional types ----


def test_query_tuple_and_fixed_string(client):
    r = client.query("SELECT tuple(1, 'a', 3.14) AS t, toFixedString('xyz', 4) AS fs")
    t, fs = r.result_rows[0]
    assert t == (1, "a", 3.14)
    assert fs == b"xyz\x00"


def test_query_uuid(client):
    val = "550e8400-e29b-41d4-a716-446655440000"
    r = client.query(f"SELECT toUUID('{val}') AS u")
    assert r.result_rows == [(UUID(val),)]


def test_query_ipv4_ipv6(client):
    r = client.query("SELECT toIPv4('127.0.0.1') AS v4, toIPv6('::1') AS v6")
    v4, v6 = r.result_rows[0]
    import ipaddress

    assert v4 == ipaddress.IPv4Address("127.0.0.1")
    assert v6 == ipaddress.IPv6Address("::1")


def test_query_enum(client):
    r = client.query("SELECT CAST('a' AS Enum8('a' = 1, 'b' = 2)) AS e")
    assert r.result_rows == [("a",)]


def test_query_datetime64_with_tz(client):
    r = client.query("SELECT toDateTime64('2026-05-19 10:30:00.123456', 6, 'America/New_York') AS dt")
    (dt,) = r.result_rows[0]
    assert dt.year == 2026 and dt.microsecond == 123456


def test_query_nan_handling(client):
    r = client.query("SELECT CAST('nan' AS Float64) AS x, CAST('-inf' AS Float64) AS y")
    x, y = r.result_rows[0]
    assert x != x  # NaN
    assert y == float("-inf")


# ---- parameter binding ----


def test_query_with_parameters(client):
    r = client.query("SELECT {x:Int32} AS x, {name:String} AS name", parameters={"x": 13, "name": "user_1"})
    assert r.result_rows == [(13, "user_1")]


# ---- transport-only settings don't get persisted ----


def test_transport_only_setting_not_persisted_to_session(client):
    # session_id is a transport-only key; ChdbClient should accept it but NOT emit
    # SET session_id=... to chdb (which would either error or apply a meaningless setting).
    before = _read_session_setting(client, "session_id")
    client.set_client_setting("session_id", "abc-123")
    after = _read_session_setting(client, "session_id")
    assert after == before
    # But the recorded client-side value is kept for inspection
    assert client.get_client_setting("session_id") == "abc-123"


# ---- DataFrame stream ----


def test_query_df_stream(client):
    pytest.importorskip("pandas")
    client.command("CREATE TABLE df_stream (id UInt32) ENGINE = Memory")
    client.insert("df_stream", [[i] for i in range(20)], column_names=["id"])
    with client.query_df_stream("SELECT id FROM df_stream SETTINGS max_block_size = 5") as stream:
        frames = list(stream)
    total = sum(len(f) for f in frames)
    assert total == 20


# ---- async additional coverage ----


def test_async_external_data_rejected():
    async def run():
        c = await clickhouse_connect.get_async_client(interface="chdb")
        try:
            from clickhouse_connect.driver.external import ExternalData

            ext = ExternalData(file_name="x.csv", data=b"1\n", fmt="CSV", structure="id UInt32")
            with pytest.raises(NotSupportedError):
                await c.query("SELECT * FROM x", external_data=ext)
        finally:
            await c.close()

    asyncio.run(run())


def test_async_query_error_propagates_as_database_error():
    async def run():
        c = await clickhouse_connect.get_async_client(interface="chdb")
        try:
            with pytest.raises(DatabaseError):
                await c.query("SELECT bad_function()")
        finally:
            await c.close()

    asyncio.run(run())


def test_async_closed_client_query_raises():
    async def run():
        c = await clickhouse_connect.get_async_client(interface="chdb")
        await c.close()
        with pytest.raises(ProgrammingError):
            await c.query("SELECT 1")

    asyncio.run(run())


def test_async_set_client_setting_is_sync(client):
    # Async client's set_client_setting is intentionally sync (no I/O wrap) for
    # symmetry with HTTP AsyncClient.
    async def run():
        c = await clickhouse_connect.get_async_client(interface="chdb")
        try:
            c.set_client_setting("max_block_size", 99)  # NOT awaited
            assert c.get_client_setting("max_block_size") == "99"
        finally:
            await c.close()

    asyncio.run(run())
