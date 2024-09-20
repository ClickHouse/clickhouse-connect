from typing import Callable

from clickhouse_connect.datatypes.format import set_write_format
from clickhouse_connect.driver import Client


def test_basic_json(test_client: Client, table_context: Callable):
    with table_context('new_json_basic', [
        'key Int32',
        'value JSON',
        'e2 Int32',
        "null_value JSON"
    ]):
        jv1 = {'key1': 337, 'value.2': 'vvvv', 'HKD@spéçiäl': 'Special K', 'blank': 'not_really_blank'}
        jv3 = {'key3': 752, 'value.2': 'v2_rules', 'blank': None}
        njv2 = {'nk1': -302, 'nk2': {'sub1': 372, 'sub2': 'a string'}}
        njv3 = {'nk1': 5832.44, 'nk2': {'sub1': 47788382, 'sub2':'sub2val', 'sub3': 'sub3str', 'space key': 'spacey'}}
        test_client.insert('new_json_basic', [
            [5, jv1, -44, None],
            [20, None, 5200, njv2],
            [25, jv3, 7302, njv3]])

        result = test_client.query('SELECT * FROM new_json_basic ORDER BY key')
        json1 = result.result_set[0][1]
        assert json1['HKD@spéçiäl'] == 'Special K'
        assert json1['key3'] == 0
        json2 = result.result_set[1][3]
        assert json2['nk1'] == -302.0
        assert json2['nk2']['sub2'] == 'a string'
        assert json2['nk2']['sub3'] is None
        json3 = result.result_set[2][1]
        assert json3['value.2'] == 'v2_rules'
        assert json3['blank'] == ''
        assert json3['key1'] == 0
        assert json3['key3'] == 752
        null_json3 = result.result_set[2][3]
        assert null_json3['nk2']['space key'] == 'spacey'

        set_write_format('JSON', 'string')
        test_client.insert('native_json_test', [[999, '{"key4": 283, "value.2": "str_value"}', 77, '{"nk1":53}']])
        result = test_client.query('SELECT value.key4, null_value.nk1 FROM native_json_test ORDER BY key')
        assert result.result_set[3][0] == 283
        assert result.result_set[3][1] == 53
