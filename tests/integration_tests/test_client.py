from decimal import Decimal
from time import sleep

from clickhouse_connect import create_client
from clickhouse_connect.driver.client import Client
from clickhouse_connect.driver.options import HAS_NUMPY, HAS_PANDAS
from clickhouse_connect.driver.query import QueryResult
from tests.integration_tests.conftest import TestConfig

CSV_CONTENT = """abc,1,1
abc,1,0
def,1,0
hij,1,1
hij,1,
klm,1,0
klm,1,"""


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


def test_decimal_conv(test_client: Client, test_table_engine: str):
    test_client.command('DROP TABLE IF EXISTS test_number_conv')
    test_client.command('CREATE TABLE test_num_conv (col1 UInt64, col2 Int32, f1 Float64)' +
                        f' Engine {test_table_engine} ORDER BY col1')
    data = [[Decimal(5), Decimal(-182), Decimal(55.2)], [Decimal(57238478234), Decimal(77), Decimal(-29.5773)]]
    test_client.insert('test_num_conv', data)
    result = test_client.query('SELECT * FROM test_num_conv').result_set
    assert result == [(5, -182, 55.2), (57238478234, 77, -29.5773)]


def test_session_params(test_config: TestConfig):
    client = create_client(interface=test_config.interface,
                           host=test_config.host,
                           port=test_config.port,
                           username=test_config.username,
                           password=test_config.password,
                           session_id='TEST_SESSION_ID')
    result = client.exec_query('SELECT number FROM system.numbers LIMIT 5',
                               settings={'query_id': 'test_session_params'}).result_set
    assert len(result) == 5
    if test_config.local:
        sleep(10)  # Allow the log entries to flush to tables
        result = client.exec_query(
            "SELECT session_id, user FROM system.session_log WHERE session_id = 'TEST_SESSION_ID' AND " +
            'event_time > now() - 30').result_set
        assert result[0] == ('TEST_SESSION_ID', test_config.username)
        result = client.exec_query(
            "SELECT query_id, user FROM system.query_log WHERE query_id = 'test_session_params' AND " +
            'event_time > now() - 30').result_set
        assert result[0] == ('test_session_params', test_config.username)


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


def test_insert_csv_format(test_client: Client, test_table_engine: str):
    test_client.command('DROP TABLE IF EXISTS test_csv')
    test_client.command(
        'CREATE TABLE test_csv ("key" String, "val1" Int32, "val2" Int32) engine=MergeTree() ORDER BY (tuple())')
    test_client.command(f'INSERT INTO test_csv ("key", "val1", "val2") FORMAT CSV', data=CSV_CONTENT)
    result = test_client.query('SELECT * from test_csv')
    compare_rows = lambda r1, r2: all([c1 == c2 for c1, c2 in zip(r1, r2)])
    assert len(result.result_set) == 7
    assert compare_rows(result.result_set[0], ['abc', 1, 1])
    assert compare_rows(result.result_set[4], ['hij', 1, 0])
