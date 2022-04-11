import random

from clickhouse_connect.datatypes.registry import get_from_name
from clickhouse_connect.driver.native import build_insert, parse_response
from tests.helpers import random_columns, random_data

TEST_RUNS = 10
TEST_COLUMNS = 16
MAX_DATA_ROWS = 4


def test_native_round_trips():
    data_rows = random.randint(1, MAX_DATA_ROWS)
    col_names, col_types = random_columns(TEST_COLUMNS)
    data = random_data(col_types, data_rows)
    col_names.insert(0, 'row_id')
    col_types.insert(0, get_from_name('UInt32'))
    assert len(data) == data_rows
    output = build_insert(data, column_names=col_names, column_types=col_types)
    result_set = parse_response(output)
    assert result_set[0] == data
