"""Functional tests for the in-process chDB backend.

These run without a ClickHouse server: the embedded chdb engine executes
everything in-process, so they live with the unit tests. The whole module is
skipped when the chdb package is not installed.
"""

import gc
import io
import os
import subprocess
import sys
import threading

import pytest

pytest.importorskip("chdb")

import clickhouse_connect
from clickhouse_connect.driver._backend.chdb_backend import ChdbBackend
from clickhouse_connect.driver._backend.contracts import SyncBackend
from clickhouse_connect.driver._backend.models import Capabilities
from clickhouse_connect.driver._chdbclient import ChdbClient
from clickhouse_connect.driver.exceptions import DatabaseError, NotSupportedError, ProgrammingError
from clickhouse_connect.driver.external import ExternalData
from clickhouse_connect.driver.summary import QuerySummary


@pytest.fixture(scope="module", name="client")
def client_fixture():
    client = clickhouse_connect.get_client(interface="chdb")
    yield client
    client.close()


class TestChdbContract:
    def test_backend_satisfies_contract(self, client):
        assert isinstance(client, ChdbClient)
        assert isinstance(client._backend, ChdbBackend)
        assert isinstance(client._backend, SyncBackend)

    def test_capabilities(self, client):
        assert client._backend.capabilities == Capabilities(native_async=False, sessions=False)

    def test_dsn_routing(self):
        second = clickhouse_connect.get_client(dsn="chdb://memory")
        try:
            assert isinstance(second, ChdbClient)
            assert second.path == ":memory:"
            assert second.command("SELECT 1") == 1
        finally:
            second.close()

    def test_dsn_database(self, client):
        client.command("CREATE DATABASE IF NOT EXISTS dsn_db")
        second = clickhouse_connect.get_client(dsn="chdb://memory/dsn_db")
        try:
            assert second.path == ":memory:"
            assert second.database == "dsn_db"
            assert second.command("SELECT currentDatabase()") == "dsn_db"
        finally:
            second.close()

    def test_path_via_dsn_query_param(self, client):
        second = clickhouse_connect.get_client(dsn="chdb://?path=:memory:")
        try:
            assert second.path == ":memory:"
            assert second.command("SELECT 1") == 1
        finally:
            second.close()

    def test_path_via_generic_args(self, client):
        second = clickhouse_connect.get_client(interface="chdb", generic_args={"path": ":memory:"})
        try:
            assert second.path == ":memory:"
            assert second.command("SELECT 1") == 1
        finally:
            second.close()

    def test_dsn_settings(self, client):
        second = clickhouse_connect.get_client(dsn="chdb://memory?ch_max_block_size=32768")
        try:
            assert second.get_client_setting("max_block_size") == "32768"
        finally:
            second.close()

    def test_unknown_database_raises(self, client):
        with pytest.raises(DatabaseError, match="UNKNOWN_DATABASE"):
            clickhouse_connect.get_client(interface="chdb", database="db_does_not_exist")

    def test_second_engine_path_rejected(self, client, tmp_path):
        # chdb allows one engine path per process; the fixture holds :memory:
        # open, so the engine rejects a different path at connect time
        with pytest.raises(ProgrammingError, match="Unable to open the chdb engine"):
            clickhouse_connect.get_client(interface="chdb", path=str(tmp_path / "other"))

    def test_connection_string_normalizes_path(self, tmp_path):
        from clickhouse_connect.driver._chdbclient import build_connection_string

        base = str(tmp_path / "db")
        # chdb compares engine paths literally, so spellings of one directory
        # must produce one connection string
        assert build_connection_string(base + "/", None) == build_connection_string(base, None)
        assert build_connection_string("rel_db", None) == build_connection_string("./rel_db", None)
        assert build_connection_string(None, None) == ":memory:"
        assert build_connection_string(":memory:", None) == ":memory:"

    def test_disk_path_clients_coexist(self, tmp_path):
        # The module fixture holds :memory: open, so a disk-path engine needs
        # its own process. Two clients on trailing-slash spellings of one
        # directory must land on one engine and see each other's data.
        script = (
            "import sys\n"
            "import clickhouse_connect\n"
            "path = sys.argv[1]\n"
            "first = clickhouse_connect.get_client(interface='chdb', path=path)\n"
            "second = clickhouse_connect.get_client(interface='chdb', path=path + '/')\n"
            "first.command('CREATE TABLE t (v UInt32) ENGINE MergeTree ORDER BY v')\n"
            "first.insert('t', [[13], [79]], column_names=['v'])\n"
            "assert second.query('SELECT sum(v) FROM t').result_rows == [(92,)]\n"
            "first.close()\n"
            "second.close()\n"
            "print('COEXIST_OK')\n"
        )
        package_root = os.path.dirname(os.path.dirname(clickhouse_connect.__file__))
        env = dict(os.environ)
        env["PYTHONPATH"] = package_root + os.pathsep + env.get("PYTHONPATH", "")
        proc = subprocess.run(
            [sys.executable, "-c", script, str(tmp_path / "db")],
            capture_output=True,
            text=True,
            timeout=120,
            env=env,
            check=False,
        )
        assert proc.returncode == 0, proc.stderr
        assert "COEXIST_OK" in proc.stdout

    def test_async_client_rejected(self):
        import asyncio

        with pytest.raises(ProgrammingError, match="async"):
            asyncio.new_event_loop().run_until_complete(clickhouse_connect.get_async_client(dsn="chdb://memory"))


class TestChdbHandshake:
    def test_server_state(self, client):
        assert client.server_version
        assert client.min_version("24.1")
        assert len(client.server_settings) > 100
        assert client.protocol_version == 0

    def test_generated_setting_defaults(self, client):
        assert client.get_client_setting("date_time_input_format") == "best_effort"
        value = client.command("SELECT value FROM system.settings WHERE name = 'date_time_input_format'")
        assert value == "best_effort"


class TestChdbQuery:
    def test_round_trip(self, client):
        result = client.query("SELECT number, toString(number) AS s FROM numbers(3)")
        assert result.result_rows == [(0, "0"), (1, "1"), (2, "2")]
        assert result.column_names == ("number", "s")

    def test_summary(self, client):
        result = client.query("SELECT number FROM numbers(10)")
        assert result.summary["read_rows"] == "10"
        assert int(result.summary["elapsed_ns"]) > 0

    def test_parameters(self, client):
        result = client.query("SELECT {v:UInt32} + 1 AS r", parameters={"v": 7})
        assert result.result_rows == [(8,)]

    def test_columns_only_probe(self, client):
        result = client.query("SELECT number, toString(number) AS s FROM numbers(10) LIMIT 0")
        assert result.result_rows == []
        assert result.column_names == ("number", "s")
        assert [ch_type.name for ch_type in result.column_types] == ["UInt64", "String"]

    def test_per_query_settings(self, client):
        result = client.query("SELECT value FROM system.settings WHERE name = 'max_threads'", settings={"max_threads": 2})
        assert result.result_rows == [("2",)]

    def test_transport_settings_stripped(self, client):
        result = client.query("SELECT 1 AS x", settings={"session_id": "ignored", "wait_end_of_query": "1"})
        assert result.result_rows == [(1,)]

    def test_error_mapping(self, client):
        with pytest.raises(DatabaseError, match="UNKNOWN_FUNCTION") as exc_info:
            client.query("SELECT no_such_function()")
        assert exc_info.value.code == 46
        assert exc_info.value.name == "UNKNOWN_FUNCTION"

    def test_external_data_rejected(self, client):
        external = ExternalData(file_name="x.tsv", fmt="TSV", structure="x String", data=b"a\n")
        with pytest.raises(NotSupportedError):
            client.query("SELECT 1", external_data=external)
        with pytest.raises(NotSupportedError):
            client.raw_query("SELECT 1", external_data=external)
        with pytest.raises(NotSupportedError):
            client.command("SELECT 1", external_data=external)


class TestChdbStreaming:
    def test_row_stream(self, client):
        with client.query_rows_stream("SELECT number FROM numbers(100000)") as stream:
            count = sum(1 for _ in stream)
        assert count == 100000
        assert client.command("SELECT 1") == 1

    def test_abandoned_stream_releases_connection(self, client):
        with client.query_rows_stream("SELECT number FROM numbers(1000000)") as stream:
            next(iter(stream))
        assert client.command("SELECT 1") == 1

    def test_cross_thread_call_waits_for_stream(self, client):
        # A second thread's call on the SAME client must block on the handle
        # lock while a stream is open (a query on a handle with an open
        # stream silently returns empty) and produce a correct result after
        results = []
        worker = threading.Thread(target=lambda: results.append(client.command("SELECT 13")))
        with client.query_rows_stream("SELECT number FROM numbers(1000000)") as stream:
            next(iter(stream))
            worker.start()
            worker.join(0.2)
            assert worker.is_alive()
        worker.join(10)
        assert not worker.is_alive()
        assert results == [13]

    def test_nested_call_during_stream_raises(self, client):
        with client.query_rows_stream("SELECT number FROM numbers(1000000)") as stream:
            next(iter(stream))
            with pytest.raises(ProgrammingError, match="streaming"):
                client.command("SELECT 1")
        assert client.command("SELECT 1") == 1

    def test_close_with_open_stream(self, client):
        scoped = clickhouse_connect.get_client(interface="chdb")
        stream = scoped.raw_stream("SELECT number FROM numbers(1000000)", fmt="Native")
        assert len(stream.read(1024)) > 0
        scoped.close()
        with pytest.raises(ValueError, match="closed"):
            stream.read()
        assert client.command("SELECT 1") == 1

    def test_mid_stream_failure(self, client):
        from clickhouse_connect.driver.exceptions import StreamFailureError

        with (
            pytest.raises(StreamFailureError),
            client.query_rows_stream(
                "SELECT number, intDiv(1, number - 5000) AS x FROM numbers(100000)",
                settings={"max_block_size": 100},
            ) as stream,
        ):
            for _ in stream:
                pass
        assert client.command("SELECT 1") == 1


class TestChdbCommand:
    def test_scalar_coercion(self, client):
        assert client.command("SELECT 37") == 37
        assert client.command("SELECT 'value'") == "value"
        assert client.command("SELECT 1, 'two'") == ["1", "two"]

    def test_ddl_returns_summary(self, client):
        result = client.command("CREATE TABLE cmd_ddl (a UInt32) ENGINE MergeTree ORDER BY a")
        assert isinstance(result, QuerySummary)

    @pytest.mark.parametrize(
        "cmd",
        [
            "SELECT name FROM system.databases WHERE name = 'db_no_match_79'",
            "SHOW TABLES LIKE 'no_match_pattern_79'",
            "SELECT 13 AS x WHERE 0 FORMAT JSONEachRow",
        ],
    )
    def test_command_empty_result_returns_empty_string(self, client, cmd):
        # A result-producing statement with zero rows returns an empty value,
        # not a QuerySummary, matching the HTTP client (issue #865)
        assert client.command(cmd) == ""

    def test_command_embedded_format(self, client):
        assert client.command("SELECT 13 AS x FORMAT JSONEachRow") == '{"x":13}'

    def test_command_with_data(self, client):
        client.command("CREATE TABLE cmd_data (a UInt32) ENGINE MergeTree ORDER BY a")
        client.command("INSERT INTO cmd_data FORMAT CSV", data="1\n2\n3\n")
        assert client.command("SELECT count() FROM cmd_data") == 3

    def test_command_settings_restored(self, client):
        # max_block_size has a plain numeric default that survives SET DEFAULT
        # on every chdb version, unlike expression defaults such as
        # max_threads = 'auto(N)'
        probe = "SELECT value, changed FROM system.settings WHERE name = 'max_block_size'"
        before = client.command(probe)
        client.command("CREATE TABLE cmd_settings (a UInt32) ENGINE MergeTree ORDER BY a", settings={"max_block_size": 1234})
        after = client.command(probe)
        assert after == before

    def test_settings_restored_after_apply_failure(self, client):
        # A mid-apply SET failure must not leak the settings applied before it
        probe = "SELECT value, changed FROM system.settings WHERE name = 'max_block_size'"
        before = client.command(probe)
        with pytest.raises(DatabaseError):
            client.command("SELECT 1", settings={"max_block_size": 4321, "max_threads": "garbage_value"})
        assert client.command(probe) == before

    def test_parameters(self, client):
        assert client.command("SELECT {v:UInt32} * 2", parameters={"v": 9}) == 18


class TestChdbInsert:
    def test_insert_and_read(self, client):
        client.command("CREATE TABLE ins_basic (a UInt32, b String) ENGINE MergeTree ORDER BY a")
        summary = client.insert("ins_basic", [[1, "x"], [2, "y"]], column_names=["a", "b"])
        assert isinstance(summary, QuerySummary)
        assert summary.written_rows == 2
        result = client.query("SELECT * FROM ins_basic ORDER BY a")
        assert result.result_rows == [(1, "x"), (2, "y")]

    def test_insert_values_query_with_settings(self, client):
        client.command("CREATE TABLE ins_values (a UInt32) ENGINE MergeTree ORDER BY a")
        client.query("INSERT INTO ins_values VALUES (1), (2)", settings={"max_block_size": 1234})
        assert client.command("SELECT count() FROM ins_values") == 2

    def test_insert_context_reuse(self, client):
        client.command("CREATE TABLE ins_ctx (a UInt32) ENGINE MergeTree ORDER BY a")
        context = client.create_insert_context("ins_ctx")
        assert [name for name in context.column_names] == ["a"]
        context.data = [[1], [2]]
        client.data_insert(context)
        context.data = [[3]]
        client.data_insert(context)
        assert client.command("SELECT count() FROM ins_ctx") == 3

    def test_insert_serialization_error(self, client):
        client.command("CREATE TABLE ins_bad (a UInt32) ENGINE MergeTree ORDER BY a")
        with pytest.raises(Exception):  # noqa: B017, PT011  (serialization error type varies by column type)
            client.insert("ins_bad", [["not a number"]], column_names=["a"])
        client.insert("ins_bad", [[5]], column_names=["a"])
        assert client.command("SELECT count() FROM ins_bad") == 1

    def test_raw_insert(self, client):
        client.command("CREATE TABLE ins_raw (a UInt32, b String) ENGINE MergeTree ORDER BY a")
        summary = client.raw_insert("ins_raw", ["a", "b"], b"7,seven\n8,eight\n", fmt="CSV")
        assert summary.written_rows == 2
        assert client.command("SELECT count() FROM ins_raw") == 2

    def test_raw_insert_requires_table(self, client):
        with pytest.raises(ProgrammingError):
            client.raw_insert(None, ["a"], b"1\n", fmt="CSV")

    def test_insert_compression_rejected(self, client):
        client.command("CREATE TABLE ins_comp (a UInt32) ENGINE MergeTree ORDER BY a")
        context = client.create_insert_context("ins_comp")
        context.data = [[1]]
        context.compression = "lz4"
        with pytest.raises(NotSupportedError):
            client.data_insert(context)


class TestChdbRaw:
    def test_raw_query_default_format(self, client):
        # The HTTP default output format is TabSeparated; parity holds here
        assert client.raw_query("SELECT 1, 'two'") == b"1\ttwo\n"

    def test_raw_query_format(self, client):
        assert client.raw_query("SELECT 1 AS x", fmt="JSONEachRow") == b'{"x":1}\n'

    def test_raw_query_settings(self, client):
        body = client.raw_query(
            "SELECT value FROM system.settings WHERE name = 'max_threads'",
            settings={"max_threads": 2},
        )
        assert body == b"2\n"
        assert client.raw_query("SELECT value FROM system.settings WHERE name = 'max_threads'") != b"2\n"

    def test_raw_stream_streams_native(self, client):
        stream = client.raw_stream("SELECT number FROM numbers(100)", fmt="Native")
        try:
            assert isinstance(stream, io.IOBase)
            assert len(stream.read()) > 0
        finally:
            stream.close()
        assert client.command("SELECT 1") == 1

    def test_raw_stream_materializes_unsafe_format(self, client):
        stream = client.raw_stream("SELECT number FROM numbers(10)", fmt="JSON")
        try:
            assert isinstance(stream, io.BytesIO)
            assert b'"rows": 10' in stream.read()
        finally:
            stream.close()

    def test_query_arrow(self, client):
        pytest.importorskip("pyarrow")
        table = client.query_arrow("SELECT number FROM numbers(4)")
        assert table.num_rows == 4
        with client.query_arrow_stream("SELECT number FROM numbers(1000)") as stream:
            assert sum(batch.num_rows for batch in stream) == 1000


class TestChdbLifecycle:
    def test_ping(self, client):
        assert client.ping() is True

    def test_database_switch(self, client):
        client.command("CREATE DATABASE IF NOT EXISTS lifecycle_db")
        client.command("CREATE TABLE lifecycle_db.t (a UInt32) ENGINE MergeTree ORDER BY a")
        original = client.database
        try:
            client.database = "lifecycle_db"
            assert client.command("SELECT count() FROM t") == 0
        finally:
            client.database = original or "default"

    def test_clients_with_different_databases(self, client):
        client.command("CREATE DATABASE IF NOT EXISTS iso_a")
        client.command("CREATE TABLE iso_a.marker (a UInt32) ENGINE MergeTree ORDER BY a")
        client.command("INSERT INTO iso_a.marker VALUES (7)")
        first = clickhouse_connect.get_client(interface="chdb", database="iso_a")
        second = clickhouse_connect.get_client(interface="chdb", database="default")
        try:
            assert first.command("SELECT a FROM marker") == 7
            with pytest.raises(DatabaseError, match="UNKNOWN_TABLE"):
                second.command("SELECT a FROM marker")
            # The interleaved default-database call must not leak into first
            assert first.command("SELECT a FROM marker") == 7
        finally:
            first.close()
            second.close()

    def test_independent_client_handles(self, client):
        second = clickhouse_connect.get_client(interface="chdb")
        assert second.command("SELECT 1") == 1
        second.close()
        assert client.command("SELECT 1") == 1

    def test_settings_isolated_per_client(self, client):
        probe = "SELECT value FROM system.settings WHERE name = 'max_block_size'"
        second = clickhouse_connect.get_client(interface="chdb", settings={"max_block_size": 1234})
        try:
            assert second.command(probe) == 1234
            assert client.command(probe) != 1234
        finally:
            second.close()

    def test_stream_does_not_block_other_clients(self, client):
        second = clickhouse_connect.get_client(interface="chdb")
        try:
            with client.query_rows_stream("SELECT number FROM numbers(1000000)") as stream:
                next(iter(stream))
                assert second.command("SELECT 79") == 79
        finally:
            second.close()
        assert client.command("SELECT 1") == 1

    def test_close_connections_is_noop(self, client):
        client.close_connections()
        assert client.command("SELECT 1") == 1

    def test_closed_client_raises(self):
        scoped = clickhouse_connect.get_client(interface="chdb")
        scoped.close()
        scoped.close()
        with pytest.raises(ProgrammingError):
            scoped.command("SELECT 1")

    def test_leaked_client_with_open_stream(self, client):
        # A client leaked without close() while its stream holds the handle
        # lock defers the connection close to the stream's release
        scoped = clickhouse_connect.get_client(interface="chdb")
        handle = scoped._backend._handle
        stream = scoped.raw_stream("SELECT number FROM numbers(1000000)", fmt="Native")
        assert len(stream.read(1024)) > 0
        del scoped
        gc.collect()
        assert handle.close_pending
        assert handle.conn is not None
        stream.close()
        assert handle.conn is None
        assert client.command("SELECT 1") == 1

    def test_leaked_client_without_streams_closes_handle(self, client):
        scoped = clickhouse_connect.get_client(interface="chdb")
        handle = scoped._backend._handle
        assert scoped.command("SELECT 1") == 1
        del scoped
        gc.collect()
        assert handle.conn is None
        assert client.command("SELECT 1") == 1

    def test_set_access_token_is_noop(self, client):
        client.set_access_token("token")
        assert client.command("SELECT 1") == 1

    def test_invalid_client_setting_raises(self, client):
        with pytest.raises(DatabaseError):
            client.set_client_setting("max_threads", "garbage_value")
        assert client.get_client_setting("max_threads") is None
