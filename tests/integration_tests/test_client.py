from clickhouse_connect.driver.client import Client
from clickhouse_connect.driver.options import HAS_NUMPY, HAS_PANDAS
from clickhouse_connect.driver.query import QueryResult


def test_query(test_client: Client):
    result = test_client.query('SELECT * FROM system.tables')
    assert len(result.result_set) > 0


def test_command(test_client: Client):
    version = test_client.command('SELECT version()')
    assert version.startswith('2')


def test_insert(test_client: Client, test_table_engine: str):
    test_client.command('DROP TABLE IF EXISTS test_system_insert')
    test_client.command(f'CREATE TABLE test_system_insert AS system.tables Engine {test_table_engine} ORDER BY name')
    tables_result = test_client.query('SELECT * from system.tables')
    test_client.insert(table='test_system_insert', column_names='*', data=tables_result.result_set)


def test_numpy(test_client: Client):
    if HAS_NUMPY:
        np_array = test_client.query_np('SELECT * FROM system.tables')
        assert len(np_array['database']) > 10


def test_pandas(test_client: Client, test_table_engine: str):
    if not HAS_PANDAS:
        return
    df = test_client.query_df('SELECT * FROM system.tables')
    test_client.command('DROP TABLE IF EXISTS test_system_insert')
    test_client.command(f'CREATE TABLE test_system_insert as system.tables Engine {test_table_engine}'
                        f' ORDER BY (database, name)')
    test_client.insert_df('test_system_insert', df)
    new_df = test_client.query_df('SELECT * FROM test_system_insert')
    assert new_df.columns.all() == df.columns.all()


def test_get_columns_only(test_client):
    result: QueryResult = test_client.query('SELECT name, database FROM system.tables LIMIT 0')
    assert result.column_names == ('name', 'database')

    result: QueryResult = test_client.query('SELECT database, engine FROM system.tables',
                                            settings={'metadata_only': True})
    assert result.column_names == ('database', 'engine')


def test_multiline_query(test_client: Client):
    result = test_client.query("""
    SELECT *
    FROM system.tables
    """)
    assert len(result.result_set) > 0


def test_query_with_inline_comment(test_client: Client):
    result = test_client.query("""
    SELECT *
    -- This is just a comment
    FROM system.tables
    """)
    assert len(result.result_set) > 0


def test_query_with_comment(test_client: Client):
    result = test_client.query("""
    SELECT *
    /* This is:
    a multiline comment */
    FROM system.tables
    """)
    assert len(result.result_set) > 0
