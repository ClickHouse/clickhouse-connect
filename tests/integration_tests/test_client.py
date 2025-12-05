from pathlib import Path
from time import sleep
from typing import Callable
import uuid

import pytest

from clickhouse_connect import create_client
from clickhouse_connect import datatypes
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


def _is_valid_uuid_v4(id_string: str) -> bool:
    """Helper function to validate that a string is a valid UUID v4"""
    try:
        parsed_uuid = uuid.UUID(id_string)
        return parsed_uuid.version == 4
    except (ValueError, AttributeError):
        return False


def test_ping(test_client: Client):
    assert test_client.ping() is True


def test_query(test_client: Client):
    result = test_client.query('SELECT * FROM system.tables')
    assert len(result.result_set) > 0
    assert result.row_count > 0
    assert result.first_item == next(result.named_results())


def test_command(test_client: Client):
    version = test_client.command('SELECT version()')
    assert int(version.split('.')[0]) >= 19


def test_client_name(test_client: Client):
    user_agent = test_client.headers['User-Agent']
    assert 'test' in user_agent
    assert 'py/' in user_agent


def test_transport_settings(test_client: Client):
    result = test_client.query('SELECT name,database FROM system.tables',
                               transport_settings={'X-Workload': 'ONLINE'})
    assert result.column_names == ('name', 'database')
    assert len(result.result_set) > 0


def test_none_database(test_client: Client):
    old_db = test_client.database
    test_db = test_client.command('select currentDatabase()')
    assert test_db == old_db
    try:
        test_client.database = None
        test_client.query('SELECT * FROM system.tables')
        test_db = test_client.command('select currentDatabase()')
        assert test_db == 'default'
        test_client.database = old_db
        test_db = test_client.command('select currentDatabase()')
        assert test_db == old_db
    finally:
        test_client.database = old_db


def test_session_params(test_config: TestConfig):
    session_id = 'TEST_SESSION_ID_' + test_config.test_database
    client = create_client(
        session_id=session_id,
        host=test_config.host,
        port=test_config.port,
        username=test_config.username,
        password=test_config.password)
    result = client.query('SELECT number FROM system.numbers LIMIT 5',
                          settings={'query_id': 'test_session_params'}).result_set
    assert len(result) == 5

    if client.min_version('21'):
        if test_config.host != 'localhost':
            return  # By default, the session log isn't enabled, so we only validate in environments we control
        sleep(10)  # Allow the log entries to flush to tables
        result = client.query(
            f"SELECT session_id, user FROM system.session_log WHERE session_id = '{session_id}' AND " +
            'event_time > now() - 30').result_set
        assert result[0] == (session_id, test_config.username)
        result = client.query(
            "SELECT query_id, user FROM system.query_log WHERE query_id = 'test_session_params' AND " +
            'event_time > now() - 30').result_set
        assert result[0] == ('test_session_params', test_config.username)


def test_dsn_config(test_config: TestConfig):
    session_id = 'TEST_DSN_SESSION_' + test_config.test_database
    dsn = (f'clickhousedb://{test_config.username}:{test_config.password}@{test_config.host}:{test_config.port}' +
           f'/{test_config.test_database}?session_id={session_id}&show_clickhouse_errors=false')
    client = create_client(dsn=dsn)
    assert client.get_client_setting('session_id') == session_id
    count = client.command('SELECT count() from system.tables')
    assert client.database == test_config.test_database
    assert count > 0
    try:
        client.query('SELECT nothing')
    except DatabaseError as ex:
        assert 'returned an error' in str(ex)
    client.close()


def test_no_columns_and_types_when_no_results(test_client: Client):
    """ In case of no results, the column names and types are not returned when FORMAT Native is set.
    This may cause a lot of confusion.

    Read more: https://github.com/ClickHouse/clickhouse-connect/issues/257
    """
    result = test_client.query('SELECT name, database, NOW() as dt FROM system.tables WHERE FALSE')
    assert result.column_names == ()
    assert result.column_types == ()
    assert result.result_set == []


def test_get_columns_only(test_client: Client):
    result = test_client.query('SELECT name, database, NOW() as dt FROM system.tables LIMIT 0')
    assert result.column_names == ('name', 'database', 'dt')
    assert len(result.column_types) == 3
    assert isinstance(result.column_types[0], datatypes.string.String)
    assert isinstance(result.column_types[1], datatypes.string.String)
    assert isinstance(result.column_types[2], datatypes.temporal.DateTime)
    assert len(result.result_set) == 0

    test_client.query('CREATE TABLE IF NOT EXISTS test_zero_insert (v Int8) ENGINE MergeTree() ORDER BY tuple()')
    test_client.query('INSERT INTO test_zero_insert SELECT 1 LIMIT 0')


def test_no_limit(test_client: Client):
    old_limit = test_client.query_limit
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
    # Test that non-SELECT and non-INSERT statements are treated as commands and
    # just return the QueryResult metadata
    result = test_client.query("SET count_distinct_implementation = 'uniq'")
    assert 'query_id' in result.first_item


def test_show_create(test_client: Client):
    if not test_client.min_version('21'):
        pytest.skip(f'Not supported server version {test_client.server_version}')
    result = test_client.query('SHOW CREATE TABLE system.tables')
    result.close()
    assert 'statement' in result.column_names


def test_empty_result(test_client: Client):
    assert len(test_client.query("SELECT * FROM system.tables WHERE name = '_NOT_A THING'").result_rows) == 0


def test_temporary_tables(test_client: Client, test_config: TestConfig):
    if test_config.cloud:
        pytest.skip("Skipping temporary tables test in cloud env")

    session_id = test_client.get_client_setting("session_id")
    session_settings = {"session_id": session_id}
    test_client.command("""
                        CREATE
                        TEMPORARY TABLE temp_test_table
            (
                field1 String,
                field2 String
            )""", settings=session_settings)

    test_client.command(
        "INSERT INTO temp_test_table (field1, field2) VALUES ('test1', 'test2'), ('test3', 'test4')",
        settings=session_settings,
    )
    df = test_client.query_df('SELECT * FROM temp_test_table', settings=session_settings)
    test_client.insert_df('temp_test_table', df, settings=session_settings)
    df = test_client.query_df('SELECT * FROM temp_test_table', settings=session_settings)
    assert len(df['field1']) == 4
    test_client.command('DROP TABLE IF EXISTS temp_test_table', settings=session_settings)


def test_str_as_bytes(test_client: Client, table_context: Callable):
    with table_context('test_insert_bytes', ['key UInt32', 'byte_str String', 'n_byte_str Nullable(String)']):
        test_client.insert('test_insert_bytes', [[0, 'str_0', 'n_str_0'], [1, 'str_1', 'n_str_0']])
        test_client.insert('test_insert_bytes', [[2, 'str_2'.encode('ascii'), 'n_str_2'.encode()],
                                                 [3, b'str_3', b'str_3'],
                                                 [4, bytearray([5, 120, 24]), bytes([16, 48, 52])],
                                                 [5, b'', None]
                                                 ])
        result_set = test_client.query('SELECT * FROM test_insert_bytes ORDER BY key').result_columns
        assert result_set[1][0] == 'str_0'
        assert result_set[1][3] == 'str_3'
        assert result_set[2][5] is None
        assert result_set[1][4].encode() == b'\x05\x78\x18'
        result_set = test_client.query('SELECT * FROM test_insert_bytes ORDER BY key',
                                       query_formats={'String': 'bytes'}).result_columns
        assert result_set[1][0] == b'str_0'
        assert result_set[1][4] == b'\x05\x78\x18'
        assert result_set[2][4] == b'\x10\x30\x34'


def test_embedded_binary(test_client: Client):
    binary_params = {'$xx$': 'col1,col2\n100,700'.encode()}
    result = test_client.raw_query(
        'SELECT col2, col1 FROM format(CSVWithNames, $xx$)', parameters=binary_params)
    assert result == b'700\t100\n'

    movies_file = f'{Path(__file__).parent}/movies.parquet'
    with open(movies_file, 'rb') as f:  # read bytes
        data = f.read()
    binary_params = {'$parquet$': data}
    result = test_client.query(
        'SELECT movie, rating FROM format(Parquet, $parquet$) ORDER BY movie', parameters=binary_params)
    assert result.first_item['movie'] == '12 Angry Men'

    binary_params = {'$mult$': 'foobar'.encode()}
    result = test_client.query("SELECT $mult$ as m1, $mult$ as m2 WHERE m1 = 'foobar'", parameters=binary_params)
    assert result.first_item['m2'] == 'foobar'


def test_column_rename_setting_none(test_config: TestConfig):
    sql = "SELECT 1 as `a.b.c d_e`"
    session_id = "TEST_SESSION_ID_" + test_config.test_database
    client = create_client(
        session_id=session_id,
        host=test_config.host,
        port=test_config.port,
        username=test_config.username,
        password=test_config.password,
    )
    names = client.query(
        sql,
    ).column_names
    client.close()
    assert names[0] == "a.b.c d_e"


def test_column_rename_limit_0_path(test_config: TestConfig):
    sql = "SELECT 1 as `a.b.c d_e` LIMIT 0"
    session_id = "TEST_SESSION_ID_" + test_config.test_database
    client = create_client(
        session_id=session_id,
        host=test_config.host,
        port=test_config.port,
        username=test_config.username,
        password=test_config.password,
        rename_response_column="to_camelcase_without_prefix",
    )
    names = client.query(
        sql,
    ).column_names
    client.close()
    assert names[0] == "cDE"


def test_column_rename_data_path(test_config: TestConfig):
    sql = "SELECT 1 as `a.b.c d_e`"
    session_id = "TEST_SESSION_ID_" + test_config.test_database
    client = create_client(
        session_id=session_id,
        host=test_config.host,
        port=test_config.port,
        username=test_config.username,
        password=test_config.password,
        rename_response_column="to_camelcase_without_prefix",
    )
    names = client.query(
        sql,
    ).column_names
    client.close()
    assert names[0] == "cDE"


def test_column_rename_with_bad_option(test_config: TestConfig):
    session_id = "TEST_SESSION_ID_" + test_config.test_database

    with pytest.raises(ValueError, match="Invalid option"):
        create_client(
            session_id=session_id,
            host=test_config.host,
            port=test_config.port,
            username=test_config.username,
            password=test_config.password,
            rename_response_column="not_an_option",
        )


def test_role_setting_works(test_client: Client, test_config: TestConfig):
    if test_config.cloud:
        pytest.skip("Skipping role test in cloud mode - cannot create custom users")

    role_limited = 'limit_rows_role'
    user_limited = 'limit_rows_user'
    user_password = 'R7m!pZt9qL#x'

    test_client.command(f'CREATE ROLE IF NOT EXISTS {role_limited}')
    test_client.command(f'CREATE USER IF NOT EXISTS {user_limited} IDENTIFIED BY \'{user_password}\'')
    test_client.command(f'GRANT SELECT ON system.numbers TO {user_limited}')
    test_client.command(f'GRANT {role_limited} TO {user_limited}')
    test_client.command(f'SET DEFAULT ROLE NONE TO {user_limited}')

    client = create_client(
        host=test_client.server_host_name,
        port=test_client.url.rsplit(':', 1)[-1].split('/')[0],
        username=user_limited,
        password=user_password,
    )

    # the default should not have the role
    res = client.query('SELECT currentRoles()')
    assert res.result_rows == [([],)]

    # passing it as a per-query setting should work
    res = client.query('SELECT currentRoles()', settings={'role': role_limited})
    assert res.result_rows == [([role_limited],)]

    # passing it as a per-client setting should work
    role_client = create_client(
        host=test_client.server_host_name,
        port=test_client.url.rsplit(':', 1)[-1].split('/')[0],
        username=user_limited,
        password=user_password,
        settings={'role': role_limited},
    )
    res = role_client.query('SELECT currentRoles()')
    assert res.result_rows == [([role_limited],)]


def test_query_id_autogeneration(test_client: Client, test_table_engine: str):
    """Test that query_id is auto-generated for query(), command(), and insert() methods"""
    result = test_client.query("SELECT 1")
    assert _is_valid_uuid_v4(result.query_id)

    summary = test_client.command("DROP TABLE IF EXISTS test_query_id_nonexistent")
    assert _is_valid_uuid_v4(summary.query_id())

    test_client.command("DROP TABLE IF EXISTS test_query_id_insert")
    test_client.command(f"CREATE TABLE test_query_id_insert (id UInt32) ENGINE {test_table_engine} ORDER BY id")
    summary = test_client.insert("test_query_id_insert", [[1], [2], [3]], column_names=["id"])
    assert _is_valid_uuid_v4(summary.query_id())
    test_client.command("DROP TABLE test_query_id_insert")


def test_query_id_manual_override(test_client: Client):
    """Test that manually specified query_id is respected and not overwritten"""
    manual_query_id = "test_manual_query_id_override"
    result = test_client.query("SELECT 1", settings={"query_id": manual_query_id})
    assert result.query_id == manual_query_id


# pylint: disable=protected-access
def test_query_id_disabled(test_config: TestConfig):
    """Test that autogenerate_query_id=False works correctly"""
    client_no_autogen = create_client(
        host=test_config.host,
        port=test_config.port,
        username=test_config.username,
        password=test_config.password,
        autogenerate_query_id=False,
    )

    assert client_no_autogen._autogenerate_query_id is False

    # Even with autogen disabled, server generates a query_id
    result = client_no_autogen.query("SELECT 1")
    assert _is_valid_uuid_v4(result.query_id)

    client_no_autogen.close()


def test_query_id_in_query_logs(test_client: Client, test_config: TestConfig):
    """Test that query_id appears in ClickHouse's system.query_log for observability"""
    if test_config.cloud:
        pytest.skip("Skipping query_log test in cloud environment")

    def check_in_logs(test_query_id):
        max_retries = 30
        for _ in range(max_retries):
            log_result = test_client.query(
                "SELECT query_id FROM system.query_log WHERE query_id = {query_id} AND event_time > now() - 30 LIMIT 1",
                parameters={"query_id": test_query_id}
            )

            if len(log_result.result_set) > 0:
                assert log_result.result_set[0][0] == test_query_id
                return

            sleep(0.1)

        # If we get here, query_id never appeared in logs
        pytest.fail(f"query_id '{test_query_id}' did not appear in system.query_log after {max_retries * 0.1}s")

    # Manual override check
    test_query_id_manual = "test_query_id_in_logs"
    test_client.query("SELECT 1 as num", settings={"query_id": test_query_id_manual})
    check_in_logs(test_query_id_manual)

    # Autogen check
    result = test_client.query("SELECT 2 as num")
    test_query_id_auto = result.query_id
    check_in_logs(test_query_id_auto)
