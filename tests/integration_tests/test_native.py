import pytest

from clickhouse_connect.driver import Client


def test_low_card(test_client: Client, test_table_engine: str):
    test_client.command('DROP TABLE IF EXISTS native_test')
    test_client.command('CREATE TABLE native_test (key LowCardinality(Int32), value_1 LowCardinality(String)) ' +
                        f'Engine {test_table_engine} ORDER BY key')
    test_client.insert('native_test', [[55, 'TV1'], [-578328, 'TV38882'], [57372, 'Kabc/defXX']])
    result = test_client.query("SELECT * FROM native_test WHERE value_1 LIKE '%abc/def%'")
    assert len(result.result_set) == 1


def test_json_insert(test_client: Client, test_table_engine: str):
    if not test_client.min_version('22.6.1'):
        pytest.skip('JSON test skipped for old version {test_client.server_version}')
    test_client.command('DROP TABLE IF EXISTS native_json_test')
    test_client.command('CREATE TABLE native_json_test (key Int32, value JSON, e2 Int32)' +
                        f'Engine {test_table_engine} ORDER BY key')
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


def test_read_formats(test_client: Client, test_table_engine: str):
    pass
