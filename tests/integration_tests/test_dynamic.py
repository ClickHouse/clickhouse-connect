import datetime
from ipaddress import IPv4Address
from typing import Callable
from uuid import UUID

import pytest

from clickhouse_connect.datatypes.format import set_write_format
from clickhouse_connect.driver import Client


def test_variant(test_client: Client, table_context: Callable):
    if not test_client.get_client_setting('allow_experimental_variant_type'):
        pytest.skip(f'New Variant type not available in this version: {test_client.server_version}')
    with table_context('basic_variants', [
        'key Int32',
        'v1 Variant(UInt64, String, Array(UInt64), UUID)',
        'v2 Variant(IPv4, Decimal(10, 2))']):
        data = [[1, 58322, None],
                [2, 'a string', 55.2],
                [3, 'bef56f14-0870-4f82-a35e-9a47eff45a5b', 777.25],
                [4, [120, 250], '243.12.55.44']
                ]
        test_client.insert('basic_variants', data)
        result = test_client.query('SELECT * FROM basic_variants ORDER BY key').result_set
        assert result[2][1] == UUID('bef56f14-0870-4f82-a35e-9a47eff45a5b')
        assert result[2][2] == 777.25
        assert result[3][1] == [120, 250]
        assert result[3][2] == IPv4Address('243.12.55.44')


def test_dynamic(test_client: Client, table_context: Callable):
    if not test_client.get_client_setting('allow_experimental_dynamic_type'):
        pytest.skip(f'New Dynamic type not available in this version: {test_client.server_version}')
    with table_context('basic_dynamic', [
        'key UInt64',
        'v1 Dynamic',
        'v2 Dynamic']):
        data = [[1, 58322, 15.5],
                [3, 'bef56f14-0870-4f82-a35e-9a47eff45a5b', 777.25],
                [2, 'a string', 55.2],
                [4, [120, 250], 577.22]
                ]
        test_client.insert('basic_dynamic', data)
        result = test_client.query('SELECT * FROM basic_dynamic ORDER BY key').result_set
        assert result[2][1] == 'bef56f14-0870-4f82-a35e-9a47eff45a5b'
        assert result[3][1] == '[120, 250]'
        assert result[2][2] == '777.25'


def test_basic_json(test_client: Client, table_context: Callable):
    if not test_client.get_client_setting('allow_experimental_json_type'):
        pytest.skip(f'New JSON type not available in this version: {test_client.server_version}')
    with table_context('new_json_basic', [
        'key Int32',
        'value JSON',
        "null_value JSON"
    ]):
        jv3 = {'key3': 752, 'value.2': 'v2_rules', 'blank': None}
        jv1 = {'key1': 337, 'value.2': 'vvvv', 'HKD@spéçiäl': 'Special K', 'blank': 'not_really_blank'}
        njv2 = {'nk1': -302, 'nk2': {'sub1': 372, 'sub2': 'a string'}}
        njv3 = {'nk1': 5832.44, 'nk2': {'sub1': 47788382, 'sub2': 'sub2val', 'sub3': 'sub3str', 'space key': 'spacey'}}
        test_client.insert('new_json_basic', [
            [5, jv1, None],
            [20, None, njv2],
            [25, jv3, njv3]])

        result = test_client.query('SELECT * FROM new_json_basic ORDER BY key').result_set
        json1 = result[0][1]
        assert json1['HKD@spéçiäl'] == 'Special K'
        assert 'key3' not in json1
        json2 = result[1][2]
        assert json2['nk1'] == -302.0
        assert json2['nk2']['sub2'] == 'a string'
        assert json2['nk2'].get('sub3') is None
        json3 = result[2][1]
        assert json3['value']['2'] == 'v2_rules'
        assert 'blank' not in json3
        assert 'key1' not in json3
        assert json3['key3'] == 752
        null_json3 = result[2][2]
        assert null_json3['nk2']['space key'] == 'spacey'

        set_write_format('JSON', 'string')
        test_client.insert('new_json_basic', [[999, '{"key4": 283, "value.2": "str_value"}', '{"nk1":53}']])
        result = test_client.query('SELECT value.key4, null_value.nk1 FROM new_json_basic ORDER BY key').result_set
        assert result[3][0] == 283
        assert result[3][1] == 53


def test_typed_json(test_client: Client, table_context: Callable):
    if not test_client.get_client_setting('allow_experimental_json_type'):
        pytest.skip(f'New JSON type not available in this version: {test_client.server_version}')
    with table_context('new_json_typed', [
        'key Int32',
        'value JSON(max_dynamic_paths=150, `a.b` DateTime64(3), SKIP a.c)'
    ]):
        v1 = '{"a":{"b":"2020-10-15T10:15:44.877", "c":"skip_me"}}'
        test_client.insert('new_json_typed', [[1, v1]])
        result = test_client.query('SELECT * FROM new_json_typed ORDER BY key')
        json1 = result.result_set[0][1]
        assert json1['a']['b'] == datetime.datetime(2020, 10, 15, 10, 15, 44, 877000)
