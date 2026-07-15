from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from clickhouse_connect.driver.backend.contracts import AsyncBackend, SyncBackend
from clickhouse_connect.driver.backend.http_async import HttpAsyncBackend, _plan_files, _plan_raw_files
from clickhouse_connect.driver.backend.http_sync import HttpSyncBackend, _plan_fields
from clickhouse_connect.driver.backend.httpcommon import (
    QueryRequestPlan,
    plan_command_request,
    plan_data_insert_request,
    plan_query_request,
    plan_raw_insert_request,
    plan_raw_query_request,
)
from clickhouse_connect.driver.backend.models import Capabilities, QueryRuntime
from clickhouse_connect.driver.exceptions import ProgrammingError


def make_context(
    final_query="SELECT number FROM system.numbers",
    bind_params=None,
    external_data=None,
    is_insert=False,
    uncommented_query=None,
):
    return SimpleNamespace(
        final_query=final_query,
        bind_params=bind_params or {},
        external_data=external_data,
        is_insert=is_insert,
        uncommented_query=uncommented_query if uncommented_query is not None else final_query,
    )


def make_external_data():
    return SimpleNamespace(
        query_params={"_f1_format": "CSV", "_f1_structure": "id UInt32"},
        form_data={"_f1": ("f1.csv", b"1\n2\n")},
    )


def make_plan(form_values=None, form_files=None):
    return QueryRequestPlan(columns_only=False, params={}, headers={}, form_values=form_values, form_files=form_files)


RUNTIME = QueryRuntime(database="db1", protocol_version=54468, settings={"max_threads": "4"}, retries=2)


def plan(context, runtime=RUNTIME, **overrides):
    kwargs = {
        "form_encode_query_params": False,
        "compression": None,
        "send_comp_setting": False,
        "read_format": "Native",
        "prepped_query": context.final_query,
    }
    kwargs.update(overrides)
    return plan_query_request(context, runtime, **kwargs)


class TestMainPathPlan:
    def test_plain_body(self):
        context = make_context(bind_params={"param_id": "7"})
        result = plan(context)
        assert result.columns_only is False
        assert result.body == "SELECT number FROM system.numbers\n FORMAT Native"
        assert result.form_values is None and result.form_files is None
        assert result.headers == {"Content-Type": "text/plain; charset=utf-8"}
        assert result.params == {
            "database": "db1",
            "client_protocol_version": "54468",
            "max_threads": "4",
            "param_id": "7",
        }
        assert list(result.params) == ["database", "client_protocol_version", "max_threads", "param_id"]

    def test_form_encoded(self):
        context = make_context(bind_params={"param_id": "7"})
        result = plan(context, form_encode_query_params=True)
        assert result.body is None
        assert result.headers == {}
        assert "param_id" not in result.params
        assert result.form_values == {"query": "SELECT number FROM system.numbers\n FORMAT Native", "param_id": "7"}
        assert result.form_files == {}

    def test_external_data_without_form(self):
        external = make_external_data()
        context = make_context(bind_params={"param_id": "7"}, external_data=external)
        result = plan(context)
        assert result.body is None
        assert result.form_values is None
        assert result.form_files is external.form_data
        assert result.params["query"] == "SELECT number FROM system.numbers\n FORMAT Native"
        assert result.params["param_id"] == "7"
        assert result.params["_f1_format"] == "CSV"
        assert list(result.params) == [
            "database",
            "client_protocol_version",
            "max_threads",
            "param_id",
            "query",
            "_f1_format",
            "_f1_structure",
        ]

    def test_form_with_external_data(self):
        external = make_external_data()
        context = make_context(bind_params={"param_id": "7"}, external_data=external)
        result = plan(context, form_encode_query_params=True)
        assert "query" not in result.params
        assert result.params["_f1_format"] == "CSV"
        assert result.form_values == {"query": "SELECT number FROM system.numbers\n FORMAT Native", "param_id": "7"}
        assert result.form_files is external.form_data

    def test_bytes_query_appends_bytes_format(self):
        context = make_context(final_query=b"SELECT 1", uncommented_query="SELECT 1")
        result = plan(context, prepped_query=b"SELECT 1")
        assert result.body == b"SELECT 1\n FORMAT Native"

    def test_insert_query_keeps_prepped_query_verbatim(self):
        context = make_context(final_query="INSERT INTO t SELECT 1", is_insert=True)
        result = plan(context)
        assert result.body == "INSERT INTO t SELECT 1"

    def test_prepped_query_feeds_format_append(self):
        context = make_context(final_query="SELECT 1")
        result = plan(context, prepped_query="SELECT 1\n LIMIT 10")
        assert result.body == "SELECT 1\n LIMIT 10\n FORMAT Native"

    def test_compression_headers_and_setting(self):
        context = make_context()
        result = plan(context, compression="lz4,zstd", send_comp_setting=True)
        assert result.headers["Accept-Encoding"] == "lz4,zstd"
        assert result.params["enable_http_compression"] == "1"
        result = plan(context, compression="lz4,zstd", send_comp_setting=False)
        assert "enable_http_compression" not in result.params

    def test_empty_runtime(self):
        context = make_context()
        result = plan(context, runtime=QueryRuntime())
        assert "database" not in result.params
        assert "client_protocol_version" not in result.params


class TestProbePlan:
    def probe_context(self, **kwargs):
        kwargs.setdefault("final_query", "SELECT * FROM t LIMIT 0")
        return make_context(**kwargs)

    def test_plain_probe(self):
        context = self.probe_context(bind_params={"param_id": "7"})
        result = plan(context, compression="lz4", send_comp_setting=True)
        assert result.columns_only is True
        assert result.body == "SELECT * FROM t LIMIT 0\n FORMAT JSON"
        assert result.form_values is None and result.form_files is None
        # The probe never requests compression
        assert result.headers == {}
        assert "enable_http_compression" not in result.params
        assert result.params["param_id"] == "7"
        assert result.params["client_protocol_version"] == "54468"

    def test_form_probe(self):
        context = self.probe_context(bind_params={"param_id": "7"})
        result = plan(context, form_encode_query_params=True)
        assert result.columns_only is True
        assert result.body is None
        assert result.form_values == {"query": "SELECT * FROM t LIMIT 0\n FORMAT JSON", "param_id": "7"}
        assert result.form_files == {}

    def test_external_probe_without_form(self):
        external = make_external_data()
        context = self.probe_context(bind_params={"param_id": "7"}, external_data=external)
        result = plan(context)
        assert result.columns_only is True
        assert result.body is None
        assert result.form_values is None
        assert result.form_files is external.form_data
        assert result.params["query"] == "SELECT * FROM t LIMIT 0\n FORMAT JSON"
        assert result.params["param_id"] == "7"
        assert result.params["_f1_format"] == "CSV"

    def test_form_probe_with_external_data(self):
        external = make_external_data()
        context = self.probe_context(bind_params={"param_id": "7"}, external_data=external)
        result = plan(context, form_encode_query_params=True)
        assert result.columns_only is True
        assert result.params["_f1_format"] == "CSV"
        assert result.form_values == {"query": "SELECT * FROM t LIMIT 0\n FORMAT JSON", "param_id": "7"}
        assert result.form_files is external.form_data

    def test_probe_uses_final_query_not_prepped(self):
        context = self.probe_context()
        result = plan(context, prepped_query="SELECT * FROM t LIMIT 0\n LIMIT 100")
        assert result.body == "SELECT * FROM t LIMIT 0\n FORMAT JSON"

    def test_insert_never_probes(self):
        context = make_context(final_query="INSERT INTO t LIMIT 0", is_insert=True)
        result = plan(context)
        assert result.columns_only is False


def command_plan(bound_cmd="CREATE TABLE t (id UInt32) ENGINE Memory", **overrides):
    kwargs = {
        "bind_params": {},
        "data": None,
        "external_data": None,
        "runtime": QueryRuntime(database="db1", settings={"max_threads": "4"}),
        "transport_settings": None,
    }
    kwargs.update(overrides)
    return plan_command_request(bound_cmd, **kwargs)


class TestCommandPlan:
    def test_plain_command(self):
        result = command_plan(bind_params={"param_id": "7"})
        assert result.method == "POST"
        assert result.payload == "CREATE TABLE t (id UInt32) ENGINE Memory"
        assert result.form_files is None
        assert result.headers == {}
        assert result.params == {"param_id": "7", "database": "db1", "max_threads": "4"}
        assert list(result.params) == ["param_id", "database", "max_threads"]

    def test_str_data_payload(self):
        result = command_plan("INSERT INTO t FORMAT CSV", data="1\n2\n")
        assert result.payload == b"1\n2\n"
        assert result.headers["Content-Type"] == "text/plain; charset=utf-8"
        assert result.params["query"] == "INSERT INTO t FORMAT CSV"
        assert result.method == "POST"

    def test_bytes_data_payload(self):
        result = command_plan("INSERT INTO t FORMAT Native", data=b"\x00\x01")
        assert result.payload == b"\x00\x01"
        assert result.headers["Content-Type"] == "application/octet-stream"
        assert result.params["query"] == "INSERT INTO t FORMAT Native"

    def test_external_data(self):
        external = make_external_data()
        result = command_plan("SELECT count() FROM f1", external_data=external)
        assert result.form_files is external.form_data
        assert result.payload is None
        assert result.params["query"] == "SELECT count() FROM f1"
        assert result.params["_f1_format"] == "CSV"
        assert list(result.params) == ["_f1_format", "_f1_structure", "query", "database", "max_threads"]
        assert result.method == "POST"

    def test_external_data_with_data_raises(self):
        with pytest.raises(ProgrammingError, match="external data"):
            command_plan(external_data=make_external_data(), data="1\n")

    def test_empty_command_without_data_raises(self):
        with pytest.raises(ProgrammingError, match="without query"):
            command_plan("")

    def test_binary_bind_with_data_raises(self):
        with pytest.raises(ProgrammingError, match="Binary parameter bind"):
            command_plan(b"SELECT \x00", data="extra")

    def test_binary_bound_command_alone_is_payload(self):
        result = command_plan(b"SELECT \x00")
        assert result.payload == b"SELECT \x00"
        assert "query" not in result.params
        assert result.method == "POST"

    def test_empty_str_data_behaves_as_no_data(self):
        result = command_plan("SHOW TABLES", data="")
        assert result.payload == "SHOW TABLES"
        assert "query" not in result.params
        # The data branch's Content-Type survives even though the payload was replaced
        assert result.headers["Content-Type"] == "text/plain; charset=utf-8"
        assert result.method == "POST"

    def test_empty_bytes_data_behaves_as_no_data(self):
        result = command_plan("SHOW TABLES", data=b"")
        assert result.payload == "SHOW TABLES"
        assert "query" not in result.params
        assert result.headers["Content-Type"] == "application/octet-stream"
        assert result.method == "POST"

    def test_empty_command_with_empty_data_gets(self):
        result = command_plan("", data="")
        assert result.method == "GET"
        assert result.payload == ""
        assert "query" not in result.params

    def test_no_database_when_unset(self):
        result = command_plan(runtime=QueryRuntime(settings={}))
        assert result.params == {}
        assert result.method == "POST"

    def test_transport_settings_merge_into_headers(self):
        result = command_plan(data="1\n", transport_settings={"X-Custom": "v"})
        assert result.headers == {"Content-Type": "text/plain; charset=utf-8", "X-Custom": "v"}


class TestDataInsertPlan:
    def make_insert_context(self, compression=None, transport_settings=None):
        return SimpleNamespace(compression=compression, transport_settings=transport_settings)

    def test_plain(self):
        result = plan_data_insert_request(self.make_insert_context(), RUNTIME)
        assert result.body is None
        assert result.headers == {"Content-Type": "application/octet-stream"}
        assert result.params == {"database": "db1", "max_threads": "4"}
        assert list(result.params) == ["database", "max_threads"]

    def test_str_compression_sets_encoding(self):
        result = plan_data_insert_request(self.make_insert_context(compression="lz4"), RUNTIME)
        assert result.headers["Content-Encoding"] == "lz4"

    @pytest.mark.parametrize("compression", [False, True])
    def test_non_str_compression_omits_encoding(self, compression):
        result = plan_data_insert_request(self.make_insert_context(compression=compression), RUNTIME)
        assert "Content-Encoding" not in result.headers

    def test_transport_settings_merge(self):
        context = self.make_insert_context(transport_settings={"X-Custom": "v"})
        result = plan_data_insert_request(context, QueryRuntime())
        assert result.headers == {"Content-Type": "application/octet-stream", "X-Custom": "v"}
        assert result.params == {}


class TestRawInsertPlan:
    def plan(self, **overrides):
        kwargs = {
            "table": None,
            "column_names": None,
            "insert_block": b"data",
            "fmt": "Native",
            "compression": None,
            "runtime": RUNTIME,
            "transport_settings": None,
        }
        kwargs.update(overrides)
        return plan_raw_insert_request(**kwargs)

    def test_no_table_passes_block_through(self):
        result = self.plan()
        assert result.body == b"data"
        assert "query" not in result.params
        assert result.headers == {"Content-Type": "application/octet-stream"}
        assert list(result.params) == ["database", "max_threads"]

    def test_table_embeds_query_into_bytes_body(self):
        result = self.plan(table="t1", column_names=["a", "b"], insert_block=b"1\t2\n")
        assert result.body == b"INSERT INTO t1 (`a`, `b`) FORMAT Native\n1\t2\n"
        assert "query" not in result.params

    def test_streaming_block_moves_query_to_params(self):
        gen = (b for b in [b"x"])
        result = self.plan(table="t1", insert_block=gen)
        assert result.body is gen
        assert result.params["query"] == "INSERT INTO t1 FORMAT Native"
        assert list(result.params) == ["query", "database", "max_threads"]

    def test_compressed_block_moves_query_to_params(self):
        result = self.plan(table="t1", insert_block=b"data", compression="gzip")
        assert result.body == b"data"
        assert result.params["query"] == "INSERT INTO t1 FORMAT Native"
        assert result.headers["Content-Encoding"] == "gzip"

    def test_transport_settings_merge(self):
        result = self.plan(transport_settings={"X-Custom": "v"})
        assert result.headers == {"Content-Type": "application/octet-stream", "X-Custom": "v"}


class TestRawQueryPlan:
    def plan(
        self, final_query="SELECT 1", bind_params=None, external_data=None, runtime=RUNTIME, form_encode=False, transport_settings=None
    ):
        return plan_raw_query_request(final_query, bind_params or {}, external_data, runtime, form_encode, transport_settings)

    def test_plain(self):
        result = self.plan(bind_params={"param_id": "7"})
        assert result.body == "SELECT 1"
        assert result.form_values is None and result.form_files is None
        assert result.headers == {}
        # Raw queries put settings before the database, unlike the main query path
        assert list(result.params) == ["max_threads", "database", "param_id"]

    def test_transport_settings_become_headers(self):
        result = self.plan(transport_settings={"X-Custom": "v"})
        assert result.headers == {"X-Custom": "v"}

    def test_form_encoded(self):
        result = self.plan(bind_params={"param_id": "7"}, form_encode=True)
        assert result.body is None
        assert result.form_values == {"query": "SELECT 1", "param_id": "7"}
        assert result.form_files == {}
        assert "param_id" not in result.params

    def test_external_data(self):
        external = make_external_data()
        result = self.plan(bind_params={"param_id": "7"}, external_data=external)
        assert result.body is None
        assert result.form_values is None
        assert result.form_files is external.form_data
        assert result.params["query"] == "SELECT 1"
        assert list(result.params) == ["max_threads", "database", "param_id", "query", "_f1_format", "_f1_structure"]

    def test_form_with_external_data(self):
        external = make_external_data()
        result = self.plan(external_data=external, form_encode=True)
        assert result.form_values == {"query": "SELECT 1"}
        assert result.form_files is external.form_data
        assert result.params["_f1_format"] == "CSV"
        assert "query" not in result.params

    def test_bytes_query_with_external_data_raises(self):
        with pytest.raises(ProgrammingError, match="Binary query"):
            self.plan(final_query=b"SELECT \x00", external_data=make_external_data())

    def test_bytes_query_plain_passes_through(self):
        result = self.plan(final_query=b"SELECT \x00")
        assert result.body == b"SELECT \x00"

    def test_bytes_query_form_encoded_stays_raw_in_plan(self):
        result = self.plan(final_query=b"SELECT 1", form_encode=True)
        assert result.form_values == {"query": b"SELECT 1"}
        # The sync fields merge keeps the raw bytes; the async merge decodes (tested below)
        assert _plan_fields(result)["query"] == b"SELECT 1"

    def test_no_database_when_unset(self):
        result = self.plan(runtime=QueryRuntime(settings={}))
        assert result.params == {}


class TestAsyncRawFilesMerge:
    def test_none_when_no_form_parts(self):
        assert _plan_raw_files(make_plan()) is None

    def test_values_before_files_with_text_wrapping(self):
        files = _plan_raw_files(make_plan(form_values={"query": "SELECT 1", "param_id": 7}, form_files={"_f1": ("f1", b"x")}))
        assert list(files) == ["query", "param_id", "_f1"]
        assert files["query"] == (None, "SELECT 1")
        assert files["param_id"] == (None, "7")
        assert files["_f1"] == ("f1", b"x")

    def test_bytes_query_decoded_not_str_coerced(self):
        files = _plan_raw_files(make_plan(form_values={"query": b"SELECT 1"}))
        assert files["query"] == (None, "SELECT 1")

    def test_files_only_passthrough(self):
        files = {"_f1": ("f1", b"x")}
        assert _plan_raw_files(make_plan(form_files=files)) is files


def make_sync_backend():
    return HttpSyncBackend(
        url="http://localhost:8123",
        pool_manager=Mock(),
        owns_pool_manager=False,
        headers={},
        params={},
        timeout=Mock(),
        server_host_name=None,
        token_provider=None,
        autogenerate_query_id=False,
    )


def make_async_backend():
    return HttpAsyncBackend(
        url="http://localhost:8123",
        headers={},
        client_settings={},
        timeout=Mock(),
        connector_kwargs={},
        ssl_context=None,
        proxy_url=None,
        server_host_name=None,
        token_provider=None,
        autogenerate_query_id=False,
    )


class TestBackendContracts:
    """The HTTP backends satisfy the contracts.py protocols (the same
    conformance is enforced statically by the _contract_conformance
    functions in the backend modules) and report real capabilities."""

    def test_sync_backend_satisfies_contract(self):
        assert isinstance(make_sync_backend(), SyncBackend)

    def test_async_backend_satisfies_contract(self):
        assert isinstance(make_async_backend(), AsyncBackend)

    def test_capabilities(self):
        assert make_sync_backend().capabilities == Capabilities(native_async=False, sessions=True)
        assert make_async_backend().capabilities == Capabilities(native_async=True, sessions=True)


class TestRawExecuteFlags:
    """The raw execute wrappers pin their transport flags: raw_query waits for
    the server (default server_wait) while raw_stream streams without waiting."""

    def test_sync_execute_raw_query(self):
        backend = make_sync_backend()
        backend.request = Mock(return_value=SimpleNamespace(data=b"result"))
        result = backend.execute_raw_query("SELECT 1", {}, None, RUNTIME, {"X-Custom": "v"})
        assert result == b"result"
        args, kwargs = backend.request.call_args
        assert args == ("SELECT 1", {"max_threads": "4", "database": "db1"}, {"X-Custom": "v"})
        assert kwargs == {"fields": None, "retries": 2}

    def test_sync_execute_raw_stream(self):
        backend = make_sync_backend()
        response = SimpleNamespace()
        backend.request = Mock(return_value=response)
        assert backend.execute_raw_stream("SELECT 1", {}, None, RUNTIME, None) is response
        _, kwargs = backend.request.call_args
        assert kwargs == {"fields": None, "stream": True, "server_wait": False, "retries": 2}

    @pytest.mark.asyncio
    async def test_async_execute_raw_query(self):
        backend = make_async_backend()
        response = SimpleNamespace(read=AsyncMock(return_value=b"result"), headers={})
        backend.request = AsyncMock(return_value=response)
        result = await backend.execute_raw_query("SELECT 1", {}, None, RUNTIME, {"X-Custom": "v"})
        assert result == b"result"
        args, kwargs = backend.request.call_args
        assert args == ("SELECT 1", {"max_threads": "4", "database": "db1"})
        assert kwargs == {"headers": {"X-Custom": "v"}, "files": None, "retries": 2}

    @pytest.mark.asyncio
    async def test_async_execute_raw_stream(self):
        backend = make_async_backend()
        response = SimpleNamespace()
        backend.request = AsyncMock(return_value=response)
        assert await backend.execute_raw_stream("SELECT 1", {}, None, RUNTIME, None) is response
        _, kwargs = backend.request.call_args
        assert kwargs == {"headers": {}, "files": None, "stream": True, "server_wait": False, "retries": 2}


class TestSyncFieldsMerge:
    def test_none_when_no_form_parts(self):
        assert _plan_fields(make_plan()) is None

    def test_values_before_files(self):
        fields = _plan_fields(make_plan(form_values={"query": "SELECT 1", "param_id": "7"}, form_files={"_f1": ("f1", b"x")}))
        assert list(fields) == ["query", "param_id", "_f1"]
        assert fields["query"] == "SELECT 1"
        assert fields["_f1"] == ("f1", b"x")

    def test_files_only_passthrough_content(self):
        files = {"_f1": ("f1", b"x")}
        assert _plan_fields(make_plan(form_files=files)) == files


class TestAsyncFilesMerge:
    def test_none_when_no_form_parts(self):
        assert _plan_files(make_plan()) is None

    def test_files_before_wrapped_values(self):
        files = _plan_files(make_plan(form_values={"query": "SELECT 1", "param_id": 7}, form_files={"_f1": ("f1", b"x")}))
        assert list(files) == ["_f1", "query", "param_id"]
        assert files["_f1"] == ("f1", b"x")
        # Plain values are wrapped as text parts, coerced with str()
        assert files["query"] == (None, "SELECT 1")
        assert files["param_id"] == (None, "7")

    def test_files_only_passthrough(self):
        files = {"_f1": ("f1", b"x")}
        assert _plan_files(make_plan(form_files=files)) is files
