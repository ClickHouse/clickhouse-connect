import random

from clickhouse_connect.datatypes.registry import get_from_name
from clickhouse_connect.driver.client import Client
from clickhouse_connect.driver.ddl import TableColumnDef, create_table
from tests.helpers import random_data, random_columns

TEST_RUNS = 50
TEST_COLUMNS = 10
MAX_DATA_ROWS = 25


# pylint: disable=duplicate-code
def test_query_fuzz(test_client: Client, test_table_engine: str):
    for _ in range(TEST_RUNS):
        test_client.command('DROP TABLE IF EXISTS fuzz_test')
        data_rows = random.randint(1, MAX_DATA_ROWS)
        col_names, col_types = random_columns(TEST_COLUMNS, unsupported_types=['Date32'])
        data = random_data(col_types, data_rows)
        col_names = ('row_id',) + col_names
        col_types = (get_from_name('UInt32'),) + col_types

        col_defs = [TableColumnDef(name, ch_type) for name, ch_type in zip(col_names, col_types)]
        create_stmt = create_table('fuzz_test', col_defs, test_table_engine, {'order by': 'row_id'})
        test_client.command(create_stmt)
        test_client.insert('fuzz_test', data, col_names)

        data_result = test_client.query('SELECT * FROM fuzz_test')
        assert data_result.column_names == col_names
        assert data_result.column_types == col_types
        assert data_result.result_set == data
