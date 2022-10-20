from decimal import Decimal
from time import sleep

from clickhouse_connect import create_client
from clickhouse_connect.driver.client import Client
from clickhouse_connect.driver.exceptions import DatabaseError
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


def test_raw_insert(test_client: Client, test_table_engine: str):
    test_client.command('DROP TABLE IF EXISTS test_raw_insert')
    test_client.command(f"CREATE TABLE test_raw_insert (`weir'd` String, value String) Engine {test_table_engine}" +
                        " ORDER BY `weir'd`")
    csv = 'value1\nvalue2'
    test_client.raw_insert('test_raw_insert', ['"weir\'d"'], csv.encode(), fmt='CSV')
    result = test_client.query('SELECT * FROM test_raw_insert')
    assert result.result_set[1][0] == 'value2'

    test_client.command('TRUNCATE TABLE test_raw_insert')
    tsv = 'weird1\tvalue__`2\nweird2\tvalue77'
    test_client.raw_insert('test_raw_insert', ["`weir'd`", 'value'], tsv, fmt='TSV')
    result = test_client.query('SELECT * FROM test_raw_insert')
    assert result.result_set[0][1] == 'value__`2'
    assert result.result_set[1][1] == 'value77'


def test_decimal_conv(test_client: Client, test_table_engine: str):
    test_client.command('DROP TABLE IF EXISTS test_num_conv')
    test_client.command('CREATE TABLE test_num_conv (col1 UInt64, col2 Int32, f1 Float64)' +
                        f' Engine {test_table_engine} ORDER BY col1')
    data = [[Decimal(5), Decimal(-182), Decimal(55.2)], [Decimal(57238478234), Decimal(77), Decimal(-29.5773)]]
    test_client.insert('test_num_conv', data)
    result = test_client.query('SELECT * FROM test_num_conv').result_set
    assert result == [(5, -182, 55.2), (57238478234, 77, -29.5773)]


def test_session_params(test_config: TestConfig):
    client = create_client(
        session_id='TEST_SESSION_ID',
        host=test_config.host,
        port=test_config.port,
        username=test_config.username,
        password=test_config.password)
    result = client.query('SELECT number FROM system.numbers LIMIT 5',
                          settings={'query_id': 'test_session_params'}).result_set
    assert len(result) == 5
    if test_config.host != 'localhost':
        return  # By default, the session log isn't enabled, so we only validate in environments we control
    sleep(10)  # Allow the log entries to flush to tables
    result = client.query(
        "SELECT session_id, user FROM system.session_log WHERE session_id = 'TEST_SESSION_ID' AND " +
        'event_time > now() - 30').result_set
    assert result[0] == ('TEST_SESSION_ID', test_config.username)
    result = client.query(
        "SELECT query_id, user FROM system.query_log WHERE query_id = 'test_session_params' AND " +
        'event_time > now() - 30').result_set
    assert result[0] == ('test_session_params', test_config.username)


def test_get_columns_only(test_client):
    result = test_client.query('SELECT name, database FROM system.tables LIMIT 0')
    assert result.column_names == ('name', 'database')
    assert len(result.result_set) == 0


def test_no_limit(test_client):
    old_limit = test_client.limit
    test_client.limit = 0
    result = test_client.query('SELECT name FROM system.databases')
    assert len(result.result_set) > 0
    test_client.limit = old_limit


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
    FROM system.tables LIMIT 77
    -- A second comment
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
        'CREATE TABLE test_csv ("key" String, "val1" Int32, "val2" Int32) ' +
        f'ENGINE {test_table_engine} ORDER BY tuple()')
    sql = f'INSERT INTO test_csv ("key", "val1", "val2") FORMAT CSV {CSV_CONTENT}'
    test_client.command(sql)
    result = test_client.query('SELECT * from test_csv')

    def compare_rows(row_1, row_2):
        return all(c1 == c2 for c1, c2 in zip(row_1, row_2))

    assert len(result.result_set) == 7
    assert compare_rows(result.result_set[0], ['abc', 1, 1])
    assert compare_rows(result.result_set[4], ['hij', 1, 0])


def test_non_latin_query(test_client: Client):
    result = test_client.query("SELECT database, name FROM system.tables WHERE engine_full IN ('空')")
    assert len(result.result_set) == 0


def test_error_decode(test_client: Client):
    try:
        test_client.query("SELECT database, name FROM system.tables WHERE has_own_data = '空'")
    except DatabaseError as ex:
        assert '空' in str(ex)


def test_command_as_query(test_client: Client):
    result = test_client.query('SET input_format_csv_use_best_effort_in_schema_inference=0')
    assert result.result_set[0][0] == ''
