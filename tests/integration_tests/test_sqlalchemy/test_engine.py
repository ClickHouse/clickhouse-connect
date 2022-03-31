import sqlalchemy as db

from sqlalchemy.engine.base import Engine

from tests import helpers

helpers.add_test_entries()


def test_create():
    pass


def test_basic_reflection(test_engine: Engine):
    conn = test_engine.connect()
    metadata = db.MetaData(bind=test_engine, reflect=True, schema='system')
    table = db.Table('tables', metadata)
    query = db.select([table.c.create_table_query])
    result = conn.execute(query)
    rows = result.fetchmany(100)
    print(rows)



