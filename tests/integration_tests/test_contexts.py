from collections.abc import Callable

import pytest

from clickhouse_connect.driver import Client
from clickhouse_connect.driver.query import QueryContext, QueryResult


def test_contexts(param_client: Client, call, table_context: Callable):
    with table_context("test_contexts", ["key Int32", "value1 String", "value2 String"]) as ctx:
        data = [[1, "v1", "v2"], [2, "v3", "v4"]]
        insert_context = call(param_client.create_insert_context, table=ctx.table, data=data)
        call(param_client.insert, context=insert_context)
        query_context = param_client.create_query_context(
            query=f"SELECT value1, value2 FROM {ctx.table} WHERE key = {{k:Int32}}", parameters={"k": 2}, column_oriented=True
        )
        result = call(param_client.query, context=query_context)
        assert result.result_set[1][0] == "v4"
        query_context.set_parameter("k", 1)
        result = call(param_client.query, context=query_context)
        assert result.row_count == 1
        assert result.result_set[1][0]

        data = [[1, "v5", "v6"], [2, "v7", "v8"]]
        call(param_client.insert, data=data, context=insert_context)
        result = call(param_client.query, context=query_context)
        assert result.row_count == 2

        insert_context.data = [[5, "v5", "v6"], [7, "v7", "v8"]]
        call(param_client.insert, context=insert_context)
        assert call(param_client.command, f"SELECT count() FROM {ctx.table}") == 6


def test_insert_context_data_cleared_on_failure(param_client: Client, call, table_context: Callable):
    with table_context("test_contexts", ["key Int32", "value1 String", "value2 String"]) as ctx:
        data = [[1, "v1", "v2"], [2, "v3", "v4"]]
        insert_context = call(param_client.create_insert_context, table=ctx.table, data=data)

        insert_context.table = f"{ctx.table}__does_not_exist"

        with pytest.raises(Exception):  # noqa: B017
            call(param_client.insert, context=insert_context)

        assert insert_context.data is None


def test_query_context_call_overrides(param_client: Client, call):
    query_context = param_client.create_query_context(query="SELECT {v:UInt32} AS v", parameters={"v": 13})

    result = call(param_client.query, context=query_context, parameters={"v": 79})
    assert result.result_rows == [(79,)]

    result = call(param_client.query, context=query_context, query="SELECT getSetting('max_threads') AS t", settings={"max_threads": 1})
    assert result.result_rows == [(1,)]

    assert query_context.parameters == {"v": 13}
    assert call(param_client.query, context=query_context).result_rows == [(13,)]


@pytest.mark.parametrize("method", ["query", "query_df"])
@pytest.mark.parametrize("tz_mode", ["aware", "naive_utc"])
@pytest.mark.parametrize("context_source", ["constructor", "assignment"])
def test_query_context_keeps_tz_mode(client_factory, call, method, tz_mode, context_source):
    client_mode = "naive_utc" if tz_mode == "aware" else "aware"
    client = client_factory(tz_mode=client_mode)
    query_context = client.create_query_context(
        query="SELECT toDateTime('2024-01-01 00:00:00', 'UTC')",
        tz_mode=tz_mode if context_source == "constructor" else None,
    )
    if context_source == "assignment":
        query_context.tz_mode = tz_mode

    result = call(getattr(client, method), context=query_context)
    value = result.result_rows[0][0] if method == "query" else result.iloc[0, 0]
    assert (value.tzinfo is not None) == (tz_mode == "aware")

    result = call(getattr(client, method), context=query_context, tz_mode=client_mode)
    value = result.result_rows[0][0] if method == "query" else result.iloc[0, 0]
    assert (value.tzinfo is not None) == (client_mode == "aware")
    assert query_context.tz_mode == tz_mode


@pytest.mark.parametrize("method", ["query", "query_df"])
@pytest.mark.parametrize("context_source", ["direct", "client", "direct_copy", "client_copy", "factory_copy"])
@pytest.mark.parametrize("tz_mode", ["aware", "naive_utc"])
def test_query_context_uses_client_tz_mode(client_factory, call, method, context_source, tz_mode):
    client = client_factory(tz_mode=tz_mode)
    query = "SELECT toDateTime('2024-01-01 00:00:00', 'UTC')"
    context = QueryContext(query=query) if context_source.startswith("direct") else client.create_query_context(query=query)
    if context_source == "factory_copy":
        context = client.create_query_context(context=context)
    elif context_source.endswith("_copy"):
        context = context.updated_copy().updated_copy()

    result = call(getattr(client, method), context=context)
    value = result.result_rows[0][0] if method == "query" else result.iloc[0, 0]
    assert (value.tzinfo is not None) == (tz_mode == "aware")

    client.tz_mode = "naive_utc" if tz_mode == "aware" else "aware"
    result = call(getattr(client, method), context=context)
    value = result.result_rows[0][0] if method == "query" else result.iloc[0, 0]
    assert (value.tzinfo is not None) == (client.tz_mode == "aware")


@pytest.mark.parametrize("binding", ["server", "client"])
def test_query_context_command(param_client: Client, call, client_mode, binding):
    table = f"test_query_context_command_{client_mode}_{binding}"
    query = (
        "CREATE TABLE {t:Identifier} (x UInt8) ENGINE Memory"
        if binding == "server"
        else f"CREATE TABLE {table} (x UInt8 DEFAULT %(v)s) ENGINE Memory"
    )
    query_context = param_client.create_query_context(query=query, parameters={"t": table} if binding == "server" else {"v": 13})
    try:
        result = call(param_client.query, context=query_context)
        assert isinstance(result, QueryResult)
        assert result.row_count == 1
        assert result.first_item["written_rows"] == 0
        assert result.first_item["query_id"]
        assert len(result.column_types) == len(result.column_names)
        assert call(param_client.command, f"EXISTS TABLE {table}") == 1
    finally:
        call(param_client.command, f"DROP TABLE IF EXISTS {table}")
