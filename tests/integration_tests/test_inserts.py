from decimal import Decimal
from typing import Callable

import pytest

from clickhouse_connect.driver.client import Client
from clickhouse_connect.driver.exceptions import DataError


def test_insert(param_client: Client, call, test_table_engine: str):
    if param_client.min_version('19'):
        call(param_client.command, 'DROP TABLE IF EXISTS test_system_insert')
    else:
        call(param_client.command, 'DROP TABLE IF EXISTS test_system_insert SYNC')
    call(param_client.command, f'CREATE TABLE test_system_insert AS system.tables Engine {test_table_engine} ORDER BY name')
    tables_result = call(param_client.query, 'SELECT * from system.tables')
    call(param_client.insert, table='test_system_insert', column_names='*', data=tables_result.result_set)
    copy_result = call(param_client.command, 'SELECT count() from test_system_insert')
    assert tables_result.row_count == copy_result
    call(param_client.command, 'DROP TABLE IF EXISTS test_system_insert')


def test_decimal_conv(param_client: Client, call, table_context: Callable):
    with table_context('test_num_conv', ['col1 UInt64', 'col2 Int32', 'f1 Float64']):
        data = [[Decimal(5), Decimal(-182), Decimal(55.2)], [Decimal(57238478234), Decimal(77), Decimal(-29.5773)]]
        call(param_client.insert, 'test_num_conv', data)
        result = call(param_client.query, 'SELECT * FROM test_num_conv').result_set
        assert result == [(5, -182, 55.2), (57238478234, 77, -29.5773)]


def test_float_decimal_conv(param_client: Client, call, table_context: Callable):
    with table_context('test_float_to_dec_conv', ['col1 Decimal32(6)','col2 Decimal32(6)', 'col3 Decimal128(6)', 'col4 Decimal128(6)']):
        data = [[0.492917, 0.49291700, 0.492917, 0.49291700]]
        call(param_client.insert, 'test_float_to_dec_conv', data)
        result = call(param_client.query, 'SELECT * FROM test_float_to_dec_conv').result_set
        assert result == [(Decimal("0.492917"), Decimal("0.492917"), Decimal("0.492917"), Decimal("0.492917"))]


def test_bad_data_insert(param_client: Client, call, table_context: Callable):
    with table_context('test_bad_insert', ['key Int32', 'float_col Float64']):
        data = [[1, 3.22], [2, 'nope']]
        with pytest.raises(DataError, match="array"):
            call(param_client.insert, 'test_bad_insert', data)


def test_bad_strings(param_client: Client, call, table_context: Callable):
    with table_context('test_bad_strings', 'key Int32, fs FixedString(6), nsf Nullable(FixedString(4))'):
        try:
            call(param_client.insert, 'test_bad_strings', [[1, b'\x0535', None]])
        except DataError as ex:
            assert 'match' in str(ex)
        try:
            call(param_client.insert, 'test_bad_strings', [[1, b'\x0535abc', '😀🙃']])
        except DataError as ex:
            assert 'encoded' in str(ex)


def test_low_card_dictionary_size(param_client: Client, call, table_context: Callable):
    with table_context('test_low_card_dict', 'key Int32, lc LowCardinality(String)',
                       settings={'index_granularity': 65536 }):
        data = [[x, str(x)] for x in range(30000)]
        call(param_client.insert, 'test_low_card_dict', data)
        assert 30000 == call(param_client.command, 'SELECT count() FROM test_low_card_dict')


def test_column_names_spaces(param_client: Client, call, table_context: Callable):
    with table_context('test_column_spaces',
                       columns=['key 1', 'value 1'],
                       column_types=['Int32', 'String']):
        data = [[1, 'str 1'], [2, 'str 2']]
        call(param_client.insert, 'test_column_spaces', data)
        result = call(param_client.query, 'SELECT * FROM test_column_spaces').result_rows
        assert result[0][0] == 1
        assert result[1][1] == 'str 2'


def test_numeric_conversion(param_client: Client, call, table_context: Callable):
    with table_context('test_numeric_convert',
                       columns=['key Int32', 'n_int Nullable(UInt64)', 'n_flt Nullable(Float64)']):
        data = [[1, None, None], [2, '2', '5.32']]
        call(param_client.insert, 'test_numeric_convert', data)
        result = call(param_client.query, 'SELECT * FROM test_numeric_convert').result_rows
        assert result[1][1] == 2
        assert result[1][2] == float('5.32')
        call(param_client.command, 'TRUNCATE TABLE test_numeric_convert')
        data = [[0, '55', '532.48'], [1, None, None], [2, '2', '5.32']]
        call(param_client.insert, 'test_numeric_convert', data)
        result = call(param_client.query, 'SELECT * FROM test_numeric_convert').result_rows
        assert result[0][1] == 55
        assert result[0][2] == 532.48
        assert result[1][1] is None
        assert result[2][1] == 2
        assert result[2][2] == 5.32
