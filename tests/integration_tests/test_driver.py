from clickhouse_connect.driver import BaseDriver


def test_query(test_driver: BaseDriver):
    result = test_driver.query('SELECT * FROM system.tables')
    assert len(result.result_set) > 0


def test_command(test_driver: BaseDriver):
    version = test_driver.command('SELECT version()')
    assert version.startswith('2')


def test_insert(test_driver: BaseDriver):
    test_driver.command('DROP TABLE IF EXISTS test_system_insert')
    test_driver.command('CREATE TABLE test_system_insert AS system.tables Engine MergeTree() ORDER BY name')
    tables_result = test_driver.query("SELECT * from system.tables")
    test_driver.insert(table='test_system_insert', column_names='*', data=tables_result.result_set)
