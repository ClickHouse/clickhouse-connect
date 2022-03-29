import sqlalchemy as db

from sqlalchemy.engine.base import Engine

from tests import helpers

helpers.add_test_entries()


def test_create():
    pass


def test_basic_reflection(test_engine: Engine):
    conn = test_engine.connect()
    metadata = db.MetaData(bind=test_engine, reflect=True)
    table = db.Table('cell_towers', metadata)
    query = db.select([table])
    result = conn.execute(query)
    rows = result.fetchmany(100)
    print(rows)



