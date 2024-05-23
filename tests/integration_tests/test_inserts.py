from decimal import Decimal
from typing import Callable

from clickhouse_connect.driver.client import Client
from clickhouse_connect.driver.exceptions import DataError


def test_insert(test_client: Client, test_table_engine: str):
    if test_client.min_version('19'):
        test_client.command('DROP TABLE IF EXISTS test_system_insert')
    else:
        test_client.command('DROP TABLE IF EXISTS test_system_insert SYNC')
    test_client.command(f'CREATE TABLE test_system_insert AS system.tables Engine {test_table_engine} ORDER BY name')
    tables_result = test_client.query('SELECT * from system.tables')
    insert_result = test_client.insert(table='test_system_insert', column_names='*', data=tables_result.result_set)
    assert int(tables_result.summary['read_rows']) == insert_result.written_rows
    test_client.command('DROP TABLE IF EXISTS test_system_insert')


def test_decimal_conv(test_client: Client, table_context: Callable):
    with table_context('test_num_conv', ['col1 UInt64', 'col2 Int32', 'f1 Float64']):
        data = [[Decimal(5), Decimal(-182), Decimal(55.2)], [Decimal(57238478234), Decimal(77), Decimal(-29.5773)]]
        test_client.insert('test_num_conv', data)
        result = test_client.query('SELECT * FROM test_num_conv').result_set
        assert result == [(5, -182, 55.2), (57238478234, 77, -29.5773)]

def test_float_decimal_conv(test_client: Client, table_context: Callable):
    with table_context('test_float_to_dec_conv', ['col1 Decimal32(6)','col2 Decimal32(6)', 'col3 Decimal128(6)', 'col4 Decimal128(6)']):
        data = [[0.492917, 0.49291700, 0.492917, 0.49291700]]
        test_client.insert('test_float_to_dec_conv', data)
        result = test_client.query('SELECT * FROM test_float_to_dec_conv').result_set
        assert result == [(Decimal("0.492917"), Decimal("0.492917"), Decimal("0.492917"), Decimal("0.492917"))]

def test_bad_data_insert(test_client: Client, table_context: Callable):
    with table_context('test_bad_insert', ['key Int32', 'float_col Float64']):
        data = [[1, 3.22], [2, 'nope']]
        try:
            test_client.insert('test_bad_insert', data)
        except DataError as ex:
            assert 'array' in str(ex)


def test_bad_strings(test_client: Client, table_context: Callable):
    with table_context('test_bad_strings', 'key Int32, fs FixedString(6), nsf Nullable(FixedString(4))'):
        try:
            test_client.insert('test_bad_strings', [[1, b'\x0535', None]])
        except DataError as ex:
            assert 'match' in str(ex)
        try:
            test_client.insert('test_bad_strings', [[1, b'\x0535abc', '😀🙃']])
        except DataError as ex:
            assert 'encoded' in str(ex)


def test_low_card_dictionary_size(test_client: Client, table_context: Callable):
    with table_context('test_low_card_dict', 'key Int32, lc LowCardinality(String)',
                       settings={'index_granularity': 65536 }):
        data = [[x, str(x)] for x in range(30000)]
        test_client.insert('test_low_card_dict', data)
        assert 30000 == test_client.command('SELECT count() FROM test_low_card_dict')


def test_column_names_spaces(test_client: Client, table_context: Callable):
    with table_context('test_column_spaces',
                       columns=['key 1', 'value 1'],
                       column_types=['Int32', 'String']):
        data = [[1, 'str 1'], [2, 'str 2']]
        test_client.insert('test_column_spaces', data)
        result = test_client.query('SELECT * FROM test_column_spaces').result_rows
        assert result[0][0] == 1
        assert result[1][1] == 'str 2'
