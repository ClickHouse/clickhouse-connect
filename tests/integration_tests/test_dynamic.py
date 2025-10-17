import datetime
from ipaddress import IPv4Address
from typing import Callable
from uuid import UUID
import json

import pytest

from clickhouse_connect.datatypes.format import set_write_format
from clickhouse_connect.driver import Client
from tests.integration_tests.conftest import TestConfig


def type_available(test_client: Client, data_type: str):
    if test_client.get_client_setting(f'allow_experimental_{data_type}_type') is None:
        return
    setting_def = test_client.server_settings.get(f'allow_experimental_{data_type}_value', None)
    if setting_def is not None and setting_def.value == '1':
        return
    pytest.skip(f'New {data_type.upper()} type not available in this version: {test_client.server_version}')



def test_variant(test_client: Client, table_context: Callable):
    pytest.skip('Variant string inserts broken')
    type_available(test_client, 'variant')
    with table_context('basic_variants', [
        'key Int32',
        'v1 Variant(UInt64, String, Array(UInt64), UUID)',
        'v2 Variant(IPv4, Decimal(10, 2))']):
        data = [[1, 58322, None],
                [2, 'a string', 55.2],
                [3, 'bef56f14-0870-4f82-a35e-9a47eff45a5b', 777.25],
                [4, [120, 250], 88.2]
                ]
        test_client.insert('basic_variants', data)
        result = test_client.query('SELECT * FROM basic_variants ORDER BY key').result_set
        assert result[2][1] == UUID('bef56f14-0870-4f82-a35e-9a47eff45a5b')
        assert result[2][2] == 777.25
        assert result[3][1] == [120, 250]
        assert result[3][2] == IPv4Address('243.12.55.44')


def test_nested_variant(test_client: Client, table_context: Callable):
    pytest.skip('Variant string inserts broken')
    type_available(test_client, 'variant')
    with table_context('nested_variants', [
        'key Int32',
        'm1 Map(String, Variant(String, UInt128, Bool))',
        't1 Tuple(Int64, Variant(Bool, String, Int32))',
        'a1 Array(Array(Variant(String, DateTime, Float64)))',
    ]):
        data = [[1,
                 {'k1': 'string1', 'k2': 34782477743, 'k3':True},
                 (-40, True),
                 (('str3', 53.732),),
                 ],
                [2,
                 {'k1': False, 'k2': 's3872', 'k3': 100},
                 (340283, 'str'),
                 (),
                 ]
                ]
        test_client.insert('nested_variants', data)
        result = test_client.query('SELECT * FROM nested_variants ORDER BY key').result_set
        assert result[0][1]['k1'] == 'string1'
        assert result[0][1]['k2'] == 34782477743
        assert result[0][2] == (-40, True)
        assert result[0][3][0][1] == 53.732
        assert result[1][1]['k3'] == 100


def test_dynamic_nested(test_client: Client, table_context: Callable):
    type_available(test_client, 'dynamic')
    with table_context('nested_dynamics', [
        'm2 Map(String, Dynamic)'
        ], order_by='()'):
        data = [({'k4': 'string8', 'k5': 5000},)]
        test_client.insert('nested_dynamics', data)
        result = test_client.query('SELECT * FROM nested_dynamics').result_set
        assert result[0][0]['k5'] == '5000'


def test_dynamic(test_client: Client, table_context: Callable):
    type_available(test_client, 'dynamic')
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
    type_available(test_client, 'json')
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


def test_json_escaped_dots_roundtrip(test_client: Client, table_context: Callable):
    type_available(test_client, "json")
    if test_client.server_settings.get("json_type_escape_dots_in_keys") is None:
        pytest.skip("json_type_escape_dots_in_keys setting unavailable on this server version")

    # with escaping enabled dots are preserved in keys
    test_client.command("SET json_type_escape_dots_in_keys=1")
    with table_context("json_dots_escape", ["value JSON"], order_by="()"):
        payload = {"a.b": 123, "c": {"d.e": 456}}
        test_client.insert("json_dots_escape", [[payload]])
        result = test_client.query("SELECT value FROM json_dots_escape").result_set
        returned = result[0][0]

        assert "a.b" in returned
        assert "c" in returned
        assert "d.e" in returned["c"]
        assert returned["a.b"] == 123
        assert returned["c"]["d.e"] == 456

    # with escaping disabled dots create nested structure
    test_client.command("SET json_type_escape_dots_in_keys=0")
    with table_context("json_dots_no_escape", ["value JSON"], order_by="()"):
        payload = {"a.b": 789}
        test_client.insert("json_dots_no_escape", [[payload]])
        result = test_client.query("SELECT value FROM json_dots_no_escape").result_set
        returned = result[0][0]

        assert "a" in returned
        assert "b" in returned["a"]
        assert returned["a"]["b"] == 789


def test_typed_json(test_client: Client, table_context: Callable):
    type_available(test_client, 'json')
    with table_context('new_json_typed', [
        'key Int32',
        'value JSON(max_dynamic_paths=150, `a.b` DateTime64(3), SKIP a.c)'
    ]):
        v1 = '{"a":{"b":"2020-10-15T10:15:44.877", "c":"skip_me"}}'
        test_client.insert('new_json_typed', [[1, v1]])
        result = test_client.query('SELECT * FROM new_json_typed ORDER BY key')
        json1 = result.result_set[0][1]
        assert json1['a']['b'] == datetime.datetime(2020, 10, 15, 10, 15, 44, 877000)


def test_nullable_json(test_client: Client, table_context: Callable):
    if not test_client.min_version('25.2'):
        pytest.skip(f'Nullable(JSON) type not available in this version: {test_client.server_version}')
    with table_context("nullable_json", [
        "key Int32",
        "value_1 Nullable(JSON)",
        "value_2 Nullable(JSON)",
        "value_3 Nullable(JSON)"
    ]):
        v1 = {"item_a": 5, "item_b": 10}

        test_client.insert("nullable_json", [[1, v1, json.dumps(v1), None], [2, v1, None, None]])
        result = test_client.query('SELECT * FROM nullable_json ORDER BY key')
        assert result.result_set[0][1] == v1
        assert result.result_set[1][1] == v1
        assert result.result_set[0][2] == v1
        assert result.result_set[1][2] is None
        assert result.result_set[0][3] is None
        assert result.result_set[1][3] is None


def test_complex_json(test_client: Client, table_context: Callable):
    type_available(test_client, 'json')
    if not test_client.min_version('24.10'):
        pytest.skip('Complex JSON broken before 24.10')
    with table_context('new_json_complex', [
        'key Int32',
        'value Tuple(t JSON)'
        ]):
        data = [[100, ({'a': 'qwe123', 'b': 'main', 'c': None},)]]
        test_client.insert('new_json_complex', data)
        result = test_client.query('SELECT * FROM new_json_complex ORDER BY key')
        json1 = result.result_set[0][1]
        assert json1['t']['a'] == 'qwe123'


def test_json_str_time(test_client: Client, test_config: TestConfig):

    if not test_client.min_version('25.1') or test_config.cloud:
        pytest.skip('JSON string/numbers bug before 25.1, skipping')
    result = test_client.query("SELECT '{\"timerange\": \"2025-01-01T00:00:00+0000\"}'::JSON").result_set
    assert result[0][0]['timerange'] == datetime.datetime(2025, 1, 1)

    # The following query is broken -- looks like something to do with Nullable(String) in the Tuple
    # result = test_client.query("SELECT'{\"k\": [123, \"xyz\"]}'::JSON",
    #                           settings={'input_format_json_read_numbers_as_strings': 0}).result_set
