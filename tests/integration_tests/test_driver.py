from clickhouse_connect.driver import BaseDriver


def test_query(test_driver: BaseDriver):
    result = test_driver.query('SELECT * FROM system.tables')
    assert len(result.result_set) > 0


def test_insert(test_driver: BaseDriver):
    create_result = test_driver.command('CREATE TABLE test_insert AS system.tables Engine MergeTree() ORDER BY name')
    version = test_driver.command('SELECT version()')
    print(f"ClickHouse docker version: {version}")


