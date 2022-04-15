import random

from clickhouse_connect.datatypes.registry import get_from_name
from clickhouse_connect.driver.native import build_insert, parse_response
from tests.helpers import random_columns, random_data

TEST_RUNS = 200
TEST_COLUMNS = 12
MAX_DATA_ROWS = 100


# pylint: disable=duplicate-code
def test_native_round_trips():
    for _ in range(TEST_RUNS):
        data_rows = random.randint(1, MAX_DATA_ROWS)
        col_names, col_types = random_columns(TEST_COLUMNS)
        data = random_data(col_types, data_rows)
        col_names = ('row_id',) + col_names
        col_types = (get_from_name('UInt32'),) + col_types
        assert len(data) == data_rows
        output = build_insert(data, column_names=col_names, column_types=col_types)
        data_result = parse_response(output)
        assert data_result.column_names == col_names
        assert data_result.column_types == col_types
        assert data_result.result == data


def test_native_small():
    for _ in range(TEST_RUNS):
        col_names, col_types = random_columns(1)
        data = random_data(col_types, 2)
        col_names = ('row_id',) + col_names
        col_types = (get_from_name('UInt32'),) + col_types
        output = build_insert(data, column_names=col_names, column_types=col_types)
        data_result = parse_response(output)
        assert data_result.column_names == col_names
        assert data_result.column_types == col_types
        assert data_result.result == data
