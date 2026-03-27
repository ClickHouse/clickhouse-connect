from typing import Callable
import pytest

from clickhouse_connect.driver import Client


def test_contexts(param_client: Client, call, table_context: Callable):
    with table_context('test_contexts', ['key Int32', 'value1 String', 'value2 String']) as ctx:
        data = [[1, 'v1', 'v2'], [2, 'v3', 'v4']]
        insert_context = call(param_client.create_insert_context, table=ctx.table, data=data)
        call(param_client.insert, context=insert_context)
        query_context = param_client.create_query_context(
            query=f'SELECT value1, value2 FROM {ctx.table} WHERE key = {{k:Int32}}',
            parameters={'k': 2},
            column_oriented=True)
        result = call(param_client.query, context=query_context)
        assert result.result_set[1][0] == 'v4'
        query_context.set_parameter('k', 1)
        result = call(param_client.query, context=query_context)
        assert result.row_count == 1
        assert result.result_set[1][0]

        data = [[1, 'v5', 'v6'], [2, 'v7', 'v8']]
        call(param_client.insert, data=data, context=insert_context)
        result = call(param_client.query, context=query_context)
        assert result.row_count == 2

        insert_context.data = [[5, 'v5', 'v6'], [7, 'v7', 'v8']]
        call(param_client.insert, context=insert_context)
        assert call(param_client.command, f'SELECT count() FROM {ctx.table}') == 6

def test_insert_context_data_cleared_on_failure(param_client: Client, call, table_context: Callable):
    with table_context('test_contexts', ['key Int32', 'value1 String', 'value2 String']) as ctx:
        data = [[1, "v1", "v2"], [2, "v3", "v4"]]
        insert_context = call(param_client.create_insert_context, table=ctx.table, data=data)

        insert_context.table = f"{ctx.table}__does_not_exist"

        with pytest.raises(Exception):
            call(param_client.insert, context=insert_context)

        assert insert_context.data is None
