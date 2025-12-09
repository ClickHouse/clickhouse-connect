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


def test_json_with_many_paths(test_client: Client, table_context: Callable):
    """Test JSON with many dynamic paths to exercise the shared data structure.
    Validates that all keys (including those beyond max_dynamic_paths) are returned.
    """
    type_available(test_client, "json")
    with table_context("json_many_paths", ["id Int32", "data JSON(max_dynamic_paths=5)"]):  # Low limit to force shared data usage
        large_json = {f"key_{i}": f"value_{i}" for i in range(20)}
        test_client.insert("json_many_paths", [[1, large_json]])
        result = test_client.query("SELECT * FROM json_many_paths").result_set

        assert result[0][0] == 1
        returned_json = result[0][1]
        assert isinstance(returned_json, dict)
        assert len(returned_json) == 20
        for i in range(20):
            assert f"key_{i}" in returned_json
            assert returned_json[f"key_{i}"] == f"value_{i}"


def test_json_with_long_values(test_client: Client, table_context: Callable):
    """Test JSON shared data with long string values (>127 chars) to verify VarInt decoding.
    String values longer than 127 characters require multi-byte VarInt length encoding.
    """
    type_available(test_client, "json")
    with table_context("json_long_values", ["id Int32", "data JSON(max_dynamic_paths=2)"]):
        short_val = "a" * 10
        medium_val = "b" * 150
        long_val = "c" * 300

        test_json = {
            "key_0": short_val,
            "key_1": medium_val,
            "key_2": long_val,
        }
        test_client.insert("json_long_values", [[1, test_json]])
        result = test_client.query("SELECT * FROM json_long_values").result_set

        assert result[0][0] == 1
        returned_json = result[0][1]
        assert isinstance(returned_json, dict)
        assert returned_json["key_0"] == short_val
        assert returned_json["key_1"] == medium_val
        assert returned_json["key_2"] == long_val


def test_json_shared_data_primitive_types(test_client: Client, table_context: Callable):
    """
    Tests round-trip encoding/decoding of integers, floats, booleans, strings, and NULL
    when they exceed max_dynamic_paths and are stored in the shared data structure.
    """
    type_available(test_client, "json")

    # Use small max_dynamic_paths=3 to force most values into shared data
    with table_context("json_primitive_types", ["id Int32", "data JSON(max_dynamic_paths=2)"]):
        test_data = {
            "int8_val": -100,
            "int16_val": -30000,
            "int32_val": -2000000000,
            "int64_val": -9000000000000000000,
            "uint8_val": 200,
            "uint16_val": 60000,
            "uint32_val": 4000000000,
            "uint64_val": 18000000000000000000,
            "float32_val": 3.14159,
            "float64_val": 2.718281828459045,
            "bool_true": True,
            "bool_false": False,
            "string_val": "Hello, shared data!",
            "empty_string": "",
            "long_string": "x" * 200,
            "null_val": None,
            "zero_int": 0,
            "zero_float": 0.0,
            "negative_float": -123.456,
            "negative_int": -1,
        }

        test_client.insert("json_primitive_types", [[1, test_data]])
        result = test_client.query("SELECT * FROM json_primitive_types").result_set

        assert result[0][0] == 1
        returned = result[0][1]
        assert isinstance(returned, dict)

        assert returned["int8_val"] == test_data["int8_val"]
        assert returned["int16_val"] == test_data["int16_val"]
        assert returned["int32_val"] == test_data["int32_val"]
        assert returned["int64_val"] == test_data["int64_val"]
        assert returned["uint8_val"] == test_data["uint8_val"]
        assert returned["uint16_val"] == test_data["uint16_val"]
        assert returned["uint32_val"] == test_data["uint32_val"]
        assert returned["uint64_val"] == test_data["uint64_val"]
        assert returned["float32_val"] == pytest.approx(test_data["float32_val"])
        assert returned["float64_val"] == pytest.approx(test_data["float64_val"])
        assert returned["bool_true"] is test_data["bool_true"]
        assert returned["bool_false"] is test_data["bool_false"]
        assert returned["string_val"] == test_data["string_val"]
        assert returned["empty_string"] == test_data["empty_string"]
        assert returned["long_string"] == test_data["long_string"]
        assert "null_val" not in returned
        assert returned["zero_int"] == test_data["zero_int"]
        assert returned["zero_float"] == pytest.approx(test_data["zero_float"])
        assert returned["negative_float"] == pytest.approx(test_data["negative_float"])
        assert returned["negative_int"] == test_data["negative_int"]


def test_json_shared_data_multiple_rows(test_client: Client, table_context: Callable):
    """Test JSON shared data with multiple rows to ensure consistent decoding."""
    type_available(test_client, "json")

    with table_context("json_multirow", ["id Int32", "data JSON(max_dynamic_paths=2)"]):
        test_data = [
            {"a": "string_val", "b": 100, "c": 3.14, "d": True, "e": "more"},
            {"a": 42, "b": "different", "c": False, "d": 2.718, "e": -999},
            {"a": 0, "b": 0.0, "c": "", "d": None, "e": False},
        ]
        rows = [[i + 1, data] for i, data in enumerate(test_data)]

        test_client.insert("json_multirow", rows)
        result = test_client.query("SELECT * FROM json_multirow ORDER BY id").result_set
        print(result)

        # Row 1
        assert result[0][0] == 1
        row1 = result[0][1]
        assert row1["a"] == test_data[0]["a"]
        assert row1["b"] == test_data[0]["b"]
        assert row1["c"] == pytest.approx(test_data[0]["c"])
        assert row1["d"] is test_data[0]["d"]
        assert row1["e"] == test_data[0]["e"]

        # Row 2
        assert result[1][0] == 2
        row2 = result[1][1]
        assert row2["a"] == test_data[1]["a"]
        assert row2["b"] == test_data[1]["b"]
        assert row2["c"] is test_data[1]["c"]
        assert row2["d"] == pytest.approx(test_data[1]["d"])
        assert row2["e"] == test_data[1]["e"]

        # Row 3
        assert result[2][0] == 3
        row3 = result[2][1]
        assert row3["a"] == test_data[2]["a"]
        assert row3["b"] == pytest.approx(test_data[2]["b"])
        assert row3["c"] == test_data[2]["c"]
        assert "d" not in row3
        assert row3["e"] is test_data[2]["e"]

        # Query column with nulls via dot notation
        result_w_nulls = test_client.query("SELECT data.d FROM json_multirow ORDER BY id").result_set
        assert [result[0] for result in result_w_nulls] == [item["d"] for item in test_data]
