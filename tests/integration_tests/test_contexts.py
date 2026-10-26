from collections.abc import Callable

import pytest

from clickhouse_connect.driver import Client


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


def test_query_context_keeps_tz_mode(param_client: Client, call):
    query_context = param_client.create_query_context(query="SELECT toDateTime('2024-01-01 00:00:00', 'UTC')", tz_mode="aware")

    result = call(param_client.query, context=query_context)
    assert result.result_rows[0][0].tzinfo is not None
