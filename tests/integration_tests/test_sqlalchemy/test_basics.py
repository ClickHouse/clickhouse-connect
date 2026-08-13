from sqlalchemy import text
from sqlalchemy.engine import Engine

from clickhouse_connect import common

test_query = """
   -- 6dcd92a04feb50f14bbcf07c661680ba
   WITH dummy = 2
   SELECT database, name FROM system.tables LIMIT 2
   -- 6dcd92a04feb50f14bbcf07c661680ba
   """


def test_dsn_config(test_engine: Engine):
    common.set_setting("invalid_setting_action", "drop")
    client = test_engine.raw_connection().driver_connection.client
    assert client.http.connection_pool_kw["cert_reqs"] == "CERT_REQUIRED"
    assert "use_skip_indexes" in client.params
    assert client.params["http_max_field_name_size"] == "99999"
    assert client.query_limit == 2333
    assert client.compression == "zstd"


def test_cursor(test_engine: Engine):
    common.set_setting("invalid_setting_action", "drop")
    raw_conn = test_engine.raw_connection()
    cursor = raw_conn.cursor()
    sql = test_query

    cursor.execute(sql)
    assert cursor.description[0][0] == "database"
    assert cursor.description[1][1] == "String"
    assert len(cursor.data) == 2
    assert cursor.summary
    raw_conn.close()


def test_execute(test_engine: Engine):
    common.set_setting("invalid_setting_action", "drop")

    with test_engine.begin() as conn:
        sql = test_query
        rows = list(row for row in conn.execute(text(sql)))
        assert len(rows) == 2

        rows = list(row for row in conn.execute(text("DROP TABLE IF EXISTS dummy_table")))
        assert len(rows) > 0  # This is just the metadata from the "command" QueryResult

        rows = list(row for row in conn.execute(text("describe TABLE system.columns")))
        assert len(rows) > 5


def test_empty_result_with_leading_comments_keeps_metadata(test_engine: Engine):
    sql = """
        /* outer /* nested */ done */
        #! clickhouse
        # hash
        // slash
        -- dash
        WITH 13 AS value_1
        SELECT value_1 WHERE 0
        """
    with test_engine.begin() as conn:
        result = conn.execute(text(sql))

        assert list(result) == []
        assert list(result.keys()) == ["value_1"]
