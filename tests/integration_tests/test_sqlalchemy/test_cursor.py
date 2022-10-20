from sqlalchemy.engine import Engine


test_query = """
   -- 6dcd92a04feb50f14bbcf07c661680ba
   WITH dummy = 2
   SELECT database, name FROM system.tables LIMIT 2
   -- 6dcd92a04feb50f14bbcf07c661680ba
   """


def test_cursor(test_engine: Engine):
    raw_conn = test_engine.raw_connection()
    cursor = raw_conn.cursor()
    cursor.execute(test_query)
    assert cursor.description[0][0] == 'database'
    assert cursor.description[1][1] == 'String'
    assert len(getattr(cursor, 'data')) == 2
    raw_conn.close()


def test_execute(test_engine: Engine):
    connection = test_engine.connect()
    rows = list(row for row in connection.execute(test_query))
    assert len(rows) == 2

    rows = list(row for row in connection.execute('DROP TABLE IF EXISTS dummy_table'))
    assert rows[0][0] == ''

    rows = list(row for row in connection.execute('describe TABLE system.columns'))
    assert len(rows) > 5
