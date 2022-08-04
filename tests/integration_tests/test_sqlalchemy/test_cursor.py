from sqlalchemy.engine import Engine


def test_cursor_query(test_engine: Engine):
    query = """
    -- 6dcd92a04feb50f14bbcf07c661680ba
    WITH dummy = 2
    SELECT * FROM system.tables LIMIT 2
    -- 6dcd92a04feb50f14bbcf07c661680ba
    """

    connection = test_engine.connect()
    rows = list(row for row in connection.execute(query))
    assert len(rows) == 2

    rows = list(row for row in connection.execute('DROP TABLE IF EXISTS dummy_table'))
    assert rows[0][0] == 'OK'

    rows = list(row for row in connection.execute('describe TABLE system.columns'))
    assert len(rows) > 5
