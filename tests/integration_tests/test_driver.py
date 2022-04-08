from clickhouse_connect.driver import BaseDriver
from clickhouse_connect.driver.options import HAS_NUMPY, HAS_PANDAS


def test_query(test_driver: BaseDriver):
    result = test_driver.query('SELECT * FROM system.tables')
    assert len(result.result_set) > 0


def test_command(test_driver: BaseDriver):
    version = test_driver.command('SELECT version()')
    assert version.startswith('2')


def test_insert(test_driver: BaseDriver):
    test_driver.command('DROP TABLE IF EXISTS test_system_insert')
    test_driver.command('CREATE TABLE test_system_insert AS system.tables Engine MergeTree() ORDER BY name')
    tables_result = test_driver.query('SELECT * from system.tables')
    test_driver.insert(table='test_system_insert', column_names='*', data=tables_result.result_set)


def test_numpy(test_driver: BaseDriver):
    if HAS_NUMPY:
        np_array = test_driver.query_np('SELECT * FROM system.tables')
        print(np_array['name'])


def test_pandas(test_driver: BaseDriver):
    if not HAS_PANDAS:
        return
    df = test_driver.query_df('SELECT * FROM system.tables')
    print(df['database'])
    test_driver.command('DROP TABLE IF EXISTS test_system_insert')
    test_driver.command('CREATE TABLE test_system_insert as system.tables Engine Memory')
    test_driver.insert_df('test_system_insert', df)
