from sqlalchemy.engine import Engine

from clickhouse_connect import common

test_query = """
   -- 6dcd92a04feb50f14bbcf07c661680ba
   WITH dummy = 2
   SELECT database, name FROM system.tables LIMIT 2
   -- 6dcd92a04feb50f14bbcf07c661680ba
   """

test_query_ver19 = """
   -- 6dcd92a04feb50f14bbcf07c661680ba
   SELECT database, name FROM system.tables LIMIT 2
   -- 6dcd92a04feb50f14bbcf07c661680ba
   """


def test_dsn_config(test_engine: Engine):
    common.set_setting('invalid_setting_action', 'drop')
    client = test_engine.raw_connection().connection.client
    assert client.http.connection_pool_kw['cert_reqs'] == 'CERT_REQUIRED'
    if client.min_version('23'):
        assert 'use_skip_indexes' in client.params
        assert 'allow_experimental_object_type' in client.params
    assert client.query_limit == 2333
    assert client.compression == 'zstd'


def test_cursor(test_engine: Engine):
    common.set_setting('invalid_setting_action', 'drop')
    raw_conn = test_engine.raw_connection()
    cursor = raw_conn.cursor()
    sql = test_query
    if not raw_conn.connection.client.min_version('21'):
        sql = test_query_ver19

    cursor.execute(sql)
    assert cursor.description[0][0] == 'database'
    assert cursor.description[1][1] == 'String'
    assert len(getattr(cursor, 'data')) == 2
    raw_conn.close()


def test_execute(test_engine: Engine):
    common.set_setting('invalid_setting_action', 'drop')
    connection = test_engine.connect()
    sql = test_query
    if not connection.connection.connection.client.min_version('21'):
        sql = test_query_ver19

    rows = list(row for row in connection.execute(sql))
    assert len(rows) == 2

    rows = list(row for row in connection.execute('DROP TABLE IF EXISTS dummy_table'))
    assert rows[0][0] == 0

    rows = list(row for row in connection.execute('describe TABLE system.columns'))
    assert len(rows) > 5
