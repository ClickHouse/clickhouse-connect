import sqlalchemy as db

from sqlalchemy.engine.base import Engine

from clickhouse_connect.cc_sqlalchemy.datatypes.numeric import Int8, UInt16, Decimal
from clickhouse_connect.cc_sqlalchemy.ddl.custom import CreateDatabase, DropDatabase
from clickhouse_connect.cc_sqlalchemy.ddl.engine import MergeTree
from tests import helpers

helpers.add_test_entries()


def test_create_database(test_engine: Engine):
    conn = test_engine.connect()
    if not test_engine.dialect.has_database(conn, 'sqla_create_db_test'):
        conn.execute(CreateDatabase('sqla_create_db_test', 'Atomic'))
    conn.execute(DropDatabase('sqla_create_db_test'))


def test_basic_reflection(test_engine: Engine):
    conn = test_engine.connect()
    metadata = db.MetaData(bind=test_engine, reflect=True, schema='system')
    table = db.Table('tables', metadata)
    query = db.select([table.c.create_table_query])
    result = conn.execute(query)
    rows = result.fetchmany(100)
    print(rows)


def test_create_table(test_engine: Engine):
    conn = test_engine.connect()
    metadata = db.MetaData(bind=test_engine, schema='sqla_test')
    conn.execute('DROP TABLE IF EXISTS sqla_test.simple_table')
    table = db.Table('simple_table', metadata,
                     db.Column('key_col', Int8),
                     db.Column('uint_col', UInt16),
                     db.Column('dec_col', Decimal(20, 5)),
                     MergeTree(order_by=['key_col']))
    table.create(conn)
