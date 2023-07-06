import decimal
import uuid
from datetime import datetime
from ipaddress import IPv4Address, IPv6Address
from typing import Callable

import pytest

from clickhouse_connect.datatypes.format import set_default_formats, clear_default_format, set_read_format, \
    set_write_format
from clickhouse_connect.driver import Client


def test_low_card(test_client: Client, table_context: Callable):
    with table_context('native_test', ['key LowCardinality(Int32)', 'value_1 LowCardinality(String)']):
        test_client.insert('native_test', [[55, 'TV1'], [-578328, 'TV38882'], [57372, 'Kabc/defXX']])
        result = test_client.query("SELECT * FROM native_test WHERE value_1 LIKE '%abc/def%'")
        assert len(result.result_set) == 1


def test_bare_datetime64(test_client: Client, table_context: Callable):
    with table_context('bare_datetime64_test', ['key UInt32', 'dt64 DateTime64']):
        test_client.insert('bare_datetime64_test',
                           [[1, datetime(2023, 3, 25, 10, 5, 44, 772402)],
                            [2, datetime.now()],
                            [3, datetime(1965, 10, 15, 12, 0, 0)]])
        result = test_client.query('SELECT * FROM bare_datetime64_test ORDER BY key').result_rows
        assert result[0][0] == 1
        assert result[0][1] == datetime(2023, 3, 25, 10, 5, 44, 772000)
        assert result[2][1] == datetime(1965, 10, 15, 12, 0, 0)


def test_nulls(test_client: Client, table_context: Callable):
    with table_context('nullable_test', ['key UInt32', 'null_str Nullable(String)', 'null_int Nullable(Int64)']):
        test_client.insert('nullable_test', [[1, None, None],
                                             [2, 'nonnull', -57382882345666],
                                             [3, None, 5882374747732834],
                                             [4, 'nonnull2', None]])
        result = test_client.query('SELECT * FROM nullable_test ORDER BY key', use_none=False).result_rows
        assert result[2] == (3, '', 5882374747732834)
        assert result[3] == (4, 'nonnull2', 0)
        result = test_client.query('SELECT * FROM nullable_test ORDER BY key').result_rows
        assert result[1] == (2, 'nonnull', -57382882345666)
        assert result[2] == (3, None, 5882374747732834)
        assert result[3] == (4, 'nonnull2', None)


def test_json(test_client: Client, table_context: Callable):
    if not test_client.min_version('22.6.1'):
        pytest.skip('JSON test skipped for old version {test_client.server_version}')
    with table_context('native_json_test', ['key Int32', 'value JSON', 'e2 Int32']):
        jv1 = {'key1': 337, 'value.2': 'vvvv', 'HKD@spéçiäl': 'Special K', 'blank': 'not_really_blank'}
        jv3 = {'key3': 752, 'value.2': 'v2_rules', 'blank': None}
        test_client.insert('native_json_test', [[5, jv1, -44], [20, None, 5200], [25, jv3, 7302]])

        result = test_client.query('SELECT * FROM native_json_test ORDER BY key')
        json1 = result.result_set[0][1]
        assert json1['HKD@spéçiäl'] == 'Special K'
        assert json1['key3'] == 0
        json3 = result.result_set[2][1]
        assert json3['value.2'] == 'v2_rules'
        assert json3['key1'] == 0
        assert json3['key3'] == 752
        set_write_format('JSON', 'string')
        test_client.insert('native_json_test', [[999, '{"key4": 283, "value.2": "str_value"}', 77]])
        result = test_client.query('SELECT value.key4 FROM native_json_test ORDER BY key')
        assert result.result_set[3][0] == 283


def test_read_formats(test_client: Client, test_table_engine: str):
    test_client.command('DROP TABLE IF EXISTS read_format_test')
    test_client.command('CREATE TABLE read_format_test (key Int32, uuid UUID, fs FixedString(10), ipv4 IPv4,' +
                        'ip_array Array(IPv6), tup Tuple(u1 UInt64, ip2 IPv4))' +
                        f'Engine {test_table_engine} ORDER BY key')
    uuid1 = uuid.UUID('23E45688e89B-12D3-3273-426614174000')
    uuid2 = uuid.UUID('77AA3278-3728-12d3-5372-000377723832')
    row1 = (1, uuid1, '530055777k', '10.251.30.50', ['2600::', '2001:4860:4860::8844'], (7372, '10.20.30.203'))
    row2 = (2, uuid2, 'short str', '10.44.75.20', ['74:382::3332', '8700:5200::5782:3992'], (7320, '252.18.4.50'))
    test_client.insert('read_format_test', [row1, row2])

    result = test_client.query('SELECT * FROM read_format_test').result_set
    assert result[0][1] == uuid1
    assert result[1][3] == IPv4Address('10.44.75.20')
    assert result[0][2] == b'\x35\x33\x30\x30\x35\x35\x37\x37\x37\x6b'
    assert result[0][5]['u1'] == 7372
    assert result[0][5]['ip2'] == IPv4Address('10.20.30.203')

    set_default_formats('uuid', 'string', 'ip*', 'string', 'FixedString', 'string')
    result = test_client.query('SELECT * FROM read_format_test').result_set
    assert result[0][1] == '23e45688-e89b-12d3-3273-426614174000'
    assert result[1][3] == '10.44.75.20'
    assert result[0][2] == '530055777k'
    assert result[0][4][1] == '2001:4860:4860::8844'

    clear_default_format('ip*')
    result = test_client.query('SELECT * FROM read_format_test').result_set
    assert result[0][1] == '23e45688-e89b-12d3-3273-426614174000'
    assert result[1][3] == IPv4Address('10.44.75.20')
    assert result[0][4][1] == IPv6Address('2001:4860:4860::8844')
    assert result[0][2] == '530055777k'

    # Test query formats
    result = test_client.query('SELECT * FROM read_format_test', query_formats={'IP*': 'string',
                                                                                'tup': 'json'}).result_set
    assert result[1][3] == '10.44.75.20'
    assert result[0][5] == b'{"u1":7372,"ip2":"10.20.30.203"}'

    # Ensure that the query format clears
    result = test_client.query('SELECT * FROM read_format_test').result_set
    assert result[1][3] == IPv4Address('10.44.75.20')
    assert result[0][5]['ip2'] == IPv4Address('10.20.30.203')

    # Test column formats
    result = test_client.query('SELECT * FROM read_format_test', column_formats={'ipv4': 'string',
                                                                                 'tup': 'tuple'}).result_set
    assert result[1][3] == '10.44.75.20'
    assert result[0][5][1] == IPv4Address('10.20.30.203')

    # Ensure that the column format clears
    result = test_client.query('SELECT * FROM read_format_test').result_set
    assert result[1][3] == IPv4Address('10.44.75.20')
    assert result[0][5]['ip2'] == IPv4Address('10.20.30.203')

    # Test sub column formats
    set_read_format('tuple', 'tuple')
    result = test_client.query('SELECT * FROM read_format_test', column_formats={'tup': {'ip*': 'string'}}).result_set
    assert result[0][5][1] == '10.20.30.203'

    set_read_format('tuple', 'native')
    result = test_client.query('SELECT * FROM read_format_test', column_formats={'tup': {'ip*': 'string'}}).result_set
    assert result[0][5]['ip2'] == '10.20.30.203'


def test_tuple_inserts(test_client: Client, table_context: Callable):
    with table_context('insert_tuple_test', ['key Int32', 'named Tuple(fl Float64, ns Nullable(String))',
                                             'unnamed Tuple(Float64, Nullable(String))']):
        data = [[1, (3.55, 'str1'), (555, None)], [2, (-43.2, None), (0, 'str2')]]
        result = test_client.insert('insert_tuple_test', data)
        assert 2 == result.written_rows

        data = [[1, {'fl': 3.55, 'ns': 'str1'}, (555, None)], [2, {'fl': -43.2}, (0, 'str2')]]
        result = test_client.insert('insert_tuple_test', data)
        assert 2 == result.written_rows

        query_result = test_client.query('SELECT * FROM insert_tuple_test ORDER BY key').result_rows
        assert query_result[0] == query_result[1]
        assert query_result[2] == query_result[3]


def test_agg_function(test_client: Client, table_context: Callable):
    with table_context('agg_func_test', ['key Int32',
                                         'str SimpleAggregateFunction(any, String)',
                                         'lc_str SimpleAggregateFunction(any, LowCardinality(String))'],
                       engine='AggregatingMergeTree'):
        test_client.insert('agg_func_test', [(1, 'str', 'lc_str')])
        row = test_client.query('SELECT str, lc_str FROM agg_func_test').first_row
        assert row[0] == 'str'
        assert row[1] == 'lc_str'


def test_decimal_rounding(test_client: Client, table_context: Callable):
    test_vals = [732.4, 75.57, 75.49, 40.16]
    with table_context('test_decimal', ['key Int32, value Decimal(10, 2)']):
        test_client.insert('test_decimal', [[ix, x] for ix, x in enumerate(test_vals)])
        values = test_client.query('SELECT value FROM test_decimal').result_columns[0]
    with decimal.localcontext() as dec_ctx:
        dec_ctx.prec = 10
        assert [decimal.Decimal(str(x)) for x in test_vals] == values
