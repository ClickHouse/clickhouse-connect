from enum import Enum as PyEnum

import pytest
import sqlalchemy as db
from sqlalchemy import MetaData, Column, Integer, select, text

from sqlalchemy.engine.base import Engine
from sqlalchemy.orm import declarative_base

from tests.integration_tests.conftest import TestConfig
from clickhouse_connect import common
from clickhouse_connect.cc_sqlalchemy.datatypes.sqltypes import Int8, UInt16, Decimal, Enum16, Float64, Boolean, \
    FixedString, String, UInt64, UUID, DateTime, DateTime64, LowCardinality, Nullable, Array, AggregateFunction, \
    UInt32, IPv4, QBit
from clickhouse_connect.cc_sqlalchemy import final
from clickhouse_connect.cc_sqlalchemy.ddl.custom import CreateDatabase, DropDatabase
from clickhouse_connect.cc_sqlalchemy.ddl.tableengine import engine_map, ReplacingMergeTree


def test_create_database(test_engine: Engine, test_config: TestConfig, test_db: str):
    if test_db:
        common.set_setting('invalid_setting_action', 'drop')
        with test_engine.begin() as conn:
            create_db = f'create_db_{test_db}'
            if not test_engine.dialect.has_database(conn, create_db):
                if test_config.host == 'localhost' and conn.connection.driver_connection.client.min_version('20'):
                    conn.execute(CreateDatabase(create_db, 'Atomic', exists_ok=True))
                else:
                    conn.execute(CreateDatabase(create_db, exists_ok=True))
            conn.execute(DropDatabase(create_db, missing_ok=True))


class ColorEnum(PyEnum):
    RED = 1
    BLUE = 2
    TEAL = -4
    COBALT = 877


def test_create_table(test_engine: Engine, test_db: str, test_table_engine: str):
    common.set_setting('invalid_setting_action', 'drop')
    with test_engine.begin() as conn:
        table_cls = engine_map[test_table_engine]
        metadata = db.MetaData(schema=test_db)
        conn.execute(text('DROP TABLE IF EXISTS simple_table_test'))
        bool_type = Boolean
        date_tz64_type = DateTime64(3, 'Europe/Moscow')
        if not conn.connection.driver_connection.client.min_version('20'):
            bool_type = Int8
            date_tz64_type = DateTime('Europe/Moscow')
        table = db.Table('simple_table_test', metadata,
                        db.Column('key_col', Int8),
                        db.Column('uint_col', UInt16),
                        db.Column('dec_col', Decimal(38, 5)),  # Decimal128(5)
                        db.Column('enum_col', Enum16(ColorEnum)),
                        db.Column('float_col', Float64),
                        db.Column('str_col', String),
                        db.Column('fstr_col', FixedString(17)),
                        db.Column('bool_col', bool_type),
                        table_cls(('key_col', 'uint_col'), primary_key='key_col'))
        table.create(conn)
        conn.execute(text('DROP TABLE IF EXISTS advanced_table_test'))
        table = db.Table('advanced_table_test', metadata,
                        db.Column('key_col', UInt64),
                        db.Column('uuid_col', UUID),
                        db.Column('dt_col', DateTime),
                        db.Column('ip_col', IPv4),
                        db.Column('dt64_col', date_tz64_type),
                        db.Column('lc_col', LowCardinality(FixedString(16))),
                        db.Column('lc_date_col', LowCardinality(Nullable(String))),
                        db.Column('null_dt_col', Nullable(DateTime('America/Denver'))),
                        db.Column('arr_col', Array(UUID)),
                        db.Column('agg_col', AggregateFunction('uniq', LowCardinality(String))),
                        table_cls('key_col'))
        table.create(conn)


def test_declarative(test_engine: Engine, test_db: str, test_table_engine: str):
    common.set_setting('invalid_setting_action', 'drop')
    with test_engine.begin() as conn:
        conn.execute(text('DROP TABLE IF EXISTS users_test'))
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


def test_final_modifier_replacing_merge_tree(test_engine: Engine, test_db: str):
    common.set_setting('invalid_setting_action', 'drop')
    with test_engine.begin() as conn:
        metadata = db.MetaData(schema=test_db)

        test_table = db.Table(
            "test_final",
            metadata,
            Column("id", Integer),
            Column("name", String),
            Column("value", Integer),
            ReplacingMergeTree(order_by="id"),
        )

        test_table.drop(conn, checkfirst=True)
        test_table.create(conn)

        conn.execute(
            test_table.insert(),
            [
                {"id": 1, "name": "Alice", "value": 100},
                {"id": 1, "name": "Alice", "value": 200},  # Duplicate
                {"id": 2, "name": "Bob", "value": 300},
            ],
        )

        query_with_final = select(test_table).final().order_by(test_table.c.id)
        compiled = query_with_final.compile(dialect=test_engine.dialect)
        compiled_str = str(compiled)
        assert " FINAL" in compiled_str
        result = conn.execute(query_with_final)
        rows = result.fetchall()
        assert len(rows) == 2

        test_table.drop(conn)


def test_final_modifier_error_cases(test_engine: Engine, test_db: str):
    """Test FINAL modifier error handling"""
    common.set_setting("invalid_setting_action", "drop")
    with test_engine.begin() as conn:
        metadata = db.MetaData(schema=test_db)

        test_table = db.Table(
            "test_final_errors",
            metadata,
            Column("id", Integer),
            Column("name", String),
            ReplacingMergeTree(order_by="id"),
        )

        test_table.drop(conn, checkfirst=True)
        test_table.create(conn)

        # Not a Select instance
        with pytest.raises(TypeError, match="final\\(\\) expects a SQLAlchemy Select instance"):
            final("not a select")

        # No FROM clause
        query_no_from = select(db.literal(1))
        with pytest.raises(ValueError, match="final\\(\\) requires a table to apply the FINAL modifier"):
            query_no_from.final()

        # Multiple FROMs and no explicit table
        other_table = db.Table(
            "other_table",
            metadata,
            Column("id", Integer),
            Column("value", String),
            ReplacingMergeTree(order_by="id"),
        )
        other_table.drop(conn, checkfirst=True)
        other_table.create(conn)

        query_multi_from = select(test_table.c.id, other_table.c.value).select_from(test_table).select_from(other_table)

        with pytest.raises(ValueError, match="final\\(\\) is ambiguous for statements with multiple FROM clauses"):
            query_multi_from.final()

        # Invalid table parameter type
        with pytest.raises(TypeError, match="table must be a SQLAlchemy FromClause when provided"):
            query_with_from = select(test_table)
            final(query_with_from, table="not a table")

        test_table.drop(conn)
        other_table.drop(conn)


def test_qbit_table(test_engine: Engine, test_db: str, test_table_engine: str, test_config: TestConfig):
    """Test QBit type DDL and basic operations"""
    common.set_setting('invalid_setting_action', 'drop')
    with test_engine.begin() as conn:
        if test_config.cloud:
            pytest.skip('QBit type requires allow_experimental_qbit_type setting, but settings are locked in cloud')

        if not conn.connection.driver_connection.client.min_version('25.10'):
            pytest.skip('QBit type requires ClickHouse version 25.10+')

        conn.execute(text('SET allow_experimental_qbit_type = 1'))

        table_cls = engine_map[test_table_engine]
        metadata = MetaData(schema=test_db)
        conn.execute(text('DROP TABLE IF EXISTS qbit_test'))

        table = db.Table('qbit_test', metadata,
                        db.Column('id', UInt32),
                        db.Column('vector', QBit('Float32', 8)),
                        db.Column('embedding', QBit('Float32', 128)),
                        table_cls('id'))
        table.create(conn)

        # Verify table was created
        result = conn.execute(text("SHOW CREATE TABLE qbit_test"))
        create_sql = result.fetchone()[0]
        assert 'QBit(Float32, 8)' in create_sql
        assert 'QBit(Float32, 128)' in create_sql

        conn.execute(text('DROP TABLE qbit_test'))
