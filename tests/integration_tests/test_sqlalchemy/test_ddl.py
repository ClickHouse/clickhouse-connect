from enum import Enum as PyEnum

import sqlalchemy as db
from sqlalchemy import MetaData

from sqlalchemy.engine.base import Engine
from sqlalchemy.ext.declarative import declarative_base

from tests import helpers
from clickhouse_connect.cc_sqlalchemy.datatypes.sqltypes import Int8, UInt16, Decimal, Enum16, Float64, Boolean, \
    FixedString, String, UInt128, UUID, DateTime, Date32, DateTime64, LowCardinality, Nullable, Array, \
    AggregateFunction, UInt32
from clickhouse_connect.cc_sqlalchemy.ddl.custom import CreateDatabase, DropDatabase
from clickhouse_connect.cc_sqlalchemy.ddl.tableengine import engine_map

helpers.add_test_entry_points()


def test_create_database(test_engine: Engine, test_db: str):
    if test_db:
        conn = test_engine.connect()
        create_db = f'{test_db}_create_db_test'
        if not test_engine.dialect.has_database(conn, create_db):
            conn.execute(CreateDatabase(create_db, 'Atomic'))
        conn.execute(DropDatabase(create_db))


class TestEnum(PyEnum):
    RED = 1
    BLUE = 2
    TEAL = -4
    COBALT = 877


def test_create_table(test_engine: Engine, test_db: str, test_table_engine: str):
    conn = test_engine.connect()
    table_cls = engine_map[test_table_engine]
    metadata = db.MetaData(bind=test_engine, schema=test_db)
    conn.execute('DROP TABLE IF EXISTS simple_table_test')
    table = db.Table('simple_table_test', metadata,
                     db.Column('key_col', Int8),
                     db.Column('uint_col', UInt16),
                     db.Column('dec_col', Decimal(40, 5)),
                     db.Column('enum_col', Enum16(TestEnum)),
                     db.Column('float_col', Float64),
                     db.Column('str_col', String),
                     db.Column('fstr_col', FixedString(17)),
                     db.Column('bool_col', Boolean),
                     table_cls(('key_col', 'uint_col'), primary_key='key_col'))
    table.create(conn)
    conn.execute('DROP TABLE IF EXISTS advanced_table_test')
    table = db.Table('advanced_table_test', metadata,
                     db.Column('key_col', UInt128),
                     db.Column('uuid_col', UUID),
                     db.Column('dt_col', DateTime),
                     db.Column('date_col', Date32),
                     db.Column('dt64_col', DateTime64(3, 'Europe/Moscow')),
                     db.Column('lc_col', LowCardinality(FixedString(16))),
                     db.Column('lc_date_col', LowCardinality(Nullable(String))),
                     db.Column('null_dt_col', Nullable(DateTime('America/Denver'))),
                     db.Column('arr_col', Array(UUID)),
                     db.Column('agg_col', AggregateFunction('uniq', LowCardinality(String))),
                     table_cls('key_col'))
    table.create(conn)


def test_declarative(test_engine: Engine, test_db: str, test_table_engine: str):
    conn = test_engine.connect()
    conn.execute('DROP TABLE IF EXISTS users_test')
    table_cls = engine_map[test_table_engine]
    base_cls = declarative_base(metadata=MetaData(schema=test_db))

    class User(base_cls):
        __tablename__ = 'users_test'
        __table_args__ = (table_cls(order_by=['id', 'name']),)
        id = db.Column(UInt32, primary_key=True)
        name = db.Column(String)
        fullname = db.Column(String)
        nickname = db.Column(String)

    base_cls.metadata.create_all(test_engine)
    user = User(name='Alice')
    assert user.name == 'Alice'
