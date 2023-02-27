import os
import random

from clickhouse_connect.datatypes.registry import get_from_name
from clickhouse_connect.driver.client import Client
from clickhouse_connect.driver.ddl import TableColumnDef, create_table
from tests.helpers import random_data, random_columns

TEST_COLUMNS = 10
MAX_DATA_ROWS = 40


# pylint: disable=duplicate-code
def test_query_fuzz(test_client: Client, test_table_engine: str):
    test_runs = int(os.environ.get('CLICKHOUSE_CONNECT_TEST_FUZZ', '250'))
    for _ in range(test_runs):
        test_client.command('DROP TABLE IF EXISTS fuzz_test')
        data_rows = random.randint(0, MAX_DATA_ROWS)
        col_names, col_types = random_columns(TEST_COLUMNS)
        data = random_data(col_types, data_rows)
        col_names = ('row_id',) + col_names
        col_types = (get_from_name('UInt32'),) + col_types

        col_defs = [TableColumnDef(name, ch_type) for name, ch_type in zip(col_names, col_types)]
        create_stmt = create_table('fuzz_test', col_defs, test_table_engine, {'order by': 'row_id'})
        test_client.command(create_stmt, settings={'flatten_nested': 0})
        test_client.insert('fuzz_test', data, col_names)

        data_result = test_client.query('SELECT * FROM fuzz_test')
        if data_rows:
            assert data_result.column_names == col_names
            assert data_result.result_set == data
