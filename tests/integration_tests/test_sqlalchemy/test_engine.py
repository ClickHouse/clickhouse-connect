import sqlalchemy as db

from tests import helpers

helpers.add_test_entries()


def test_create():
    engine = db.create_engine('clickhousedb+connect://localhost')
    conn = engine.connect()
    metadata = db.MetaData(bind=engine, reflect=True)
    table = db.Table('cell_towers', metadata)
    query = db.select([table])
    result = conn.execute(query)
    rows = result.fetchmany(100)
    print(rows)



