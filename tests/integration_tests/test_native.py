def test_low_card(test_client):
    test_client.command('DROP TABLE IF EXISTS native_test')
    test_client.command('CREATE TABLE native_test (key LowCardinality(Int32), value_1 LowCardinality(String)) ' +
                        'Engine MergeTree ORDER BY key')
    test_client.insert('native_test', [[55, 'TV1'], [-578328, 'TV38882'], [57372, 'Kabc/defXX']])
    result = test_client.query("SELECT * FROM native_test WHERE value_1 LIKE '%abc/def%'")
    assert len(result.result_set) == 1
