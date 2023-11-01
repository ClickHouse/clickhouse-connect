import os
import random

from clickhouse_connect.datatypes.registry import get_from_name
from clickhouse_connect.driver.common import coerce_bool
from clickhouse_connect.driver.buffer import ResponseBuffer as PyBuff
from clickhouse_connect.driverc.buffer import ResponseBuffer as CBuff  # pylint: disable=no-name-in-module
from tests.helpers import random_columns, random_data, native_transform, native_insert_block, bytes_source

TEST_COLUMNS = 12
MAX_DATA_ROWS = 100

use_c = coerce_bool(os.environ.get('CLICKHOUSE_CONNECT_USE_C', True))
BuffCls = CBuff if use_c else PyBuff


# pylint: disable=duplicate-code
def test_native_round_trips():
    test_runs = int(os.environ.get('CLICKHOUSE_CONNECT_TEST_FUZZ', '200'))

    for _ in range(test_runs):
        data_rows = random.randint(1, MAX_DATA_ROWS)
        col_names, col_types = random_columns(TEST_COLUMNS)
        data = random_data(col_types, data_rows)
        col_names = ('row_id',) + col_names
        col_types = (get_from_name('UInt32'),) + col_types
        assert len(data) == data_rows
        output = native_insert_block(data, column_names=col_names, column_types=col_types)
        data_result = native_transform.parse_response(bytes_source(output, cls=BuffCls))
        assert data_result.column_names == col_names
        assert data_result.column_types == col_types
        dataset = data_result.result_set
        for row in range(data_rows):
            for col in range(TEST_COLUMNS):
                assert dataset[row][col] == data[row][col]


def test_native_small():
    test_runs = int(os.environ.get('CLICKHOUSE_CONNECT_TEST_FUZZ', '200'))
    for _ in range(test_runs):
        col_names, col_types = random_columns(1)
        data = random_data(col_types, 2)
        col_names = ('row_id',) + col_names
        col_types = (get_from_name('UInt32'),) + col_types
        output = native_insert_block(data, column_names=col_names, column_types=col_types)
        data_result = native_transform.parse_response(bytes_source(output, cls=BuffCls))
        assert data_result.column_names == col_names
        assert data_result.column_types == col_types
        assert data_result.result_set == data
