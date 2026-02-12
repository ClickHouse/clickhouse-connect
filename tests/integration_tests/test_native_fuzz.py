import asyncio
import os
import random

import pytest

from clickhouse_connect.datatypes.registry import get_from_name
from clickhouse_connect.driver.client import Client
from clickhouse_connect.driver.ddl import TableColumnDef, create_table
from tests.helpers import random_data, random_columns

TEST_COLUMNS = 10
MAX_DATA_ROWS = 40


# pylint: disable=duplicate-code
def test_query_fuzz(param_client: Client, call, test_table_engine: str, client_mode: str):
    if not param_client.min_version('21'):
        pytest.skip(f'flatten_nested setting not supported in this server version {param_client.server_version}')
    test_runs = int(os.environ.get('CLICKHOUSE_CONNECT_TEST_FUZZ', '250'))
    param_client.apply_server_timezone = True
    try:
        for _ in range(test_runs):
            call(param_client.command, 'DROP TABLE IF EXISTS fuzz_test')
            data_rows = random.randint(0, MAX_DATA_ROWS)
            col_names, col_types = random_columns(TEST_COLUMNS)
            data = random_data(col_types, data_rows, param_client.server_tz)
            col_names = ('row_id',) + col_names
            col_types = (get_from_name('UInt32'),) + col_types

            col_defs = [TableColumnDef(name, ch_type) for name, ch_type in zip(col_names, col_types)]
            create_stmt = create_table('fuzz_test', col_defs, test_table_engine, {'order by': 'row_id'})
            call(param_client.command, create_stmt, settings={'flatten_nested': 0})
            call(param_client.insert, 'fuzz_test', data, col_names)

            if client_mode == 'async':
                async def get_results():
                    result = await param_client.query('SELECT * FROM fuzz_test')
                    loop = asyncio.get_running_loop()
                    rows = await loop.run_in_executor(None, lambda: list(result.result_set))
                    return rows, result.column_names
                result_rows, result_cols = call(get_results)
            else:
                data_result = call(param_client.query, 'SELECT * FROM fuzz_test')
                result_rows = data_result.result_set
                result_cols = data_result.column_names

            if data_rows:
                assert result_cols == col_names
                assert result_rows == data
    finally:
        param_client.apply_server_timezone = False
