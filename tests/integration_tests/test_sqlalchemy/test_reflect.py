# pylint: disable=no-member
import sqlalchemy as db
from sqlalchemy.engine import Engine

from clickhouse_connect.cc_sqlalchemy.datatypes.sqltypes import UInt32


def test_basic_reflection(test_engine: Engine):
    conn = test_engine.connect()
    metadata = db.MetaData(bind=test_engine, reflect=True, schema='system')
    table = db.Table('tables', metadata)
    query = db.select([table.columns.create_table_query])
    result = conn.execute(query)
    rows = result.fetchmany(100)
    assert rows


def test_full_table_reflection(test_engine: Engine, test_db: str):
    conn = test_engine.connect()
    conn.execute(
        'CREATE TABLE IF NOT EXISTS reflect_test (key UInt32, value FixedString(20))' +
        'ENGINE MergeTree ORDER BY key')
    metadata = db.MetaData(bind=test_engine, reflect=True, schema=test_db)
    table = db.Table('reflect_test', metadata)
    assert table.columns.key.type.__class__ == UInt32
    assert table.engine.name == 'MergeTree'
