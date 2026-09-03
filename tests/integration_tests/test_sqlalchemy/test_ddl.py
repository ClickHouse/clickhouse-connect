from enum import Enum as PyEnum
from uuid import uuid4

import pytest
import sqlalchemy as db
from sqlalchemy import Column, Integer, MetaData, select, text
from sqlalchemy.engine.base import Engine
from sqlalchemy.orm import declarative_base

from clickhouse_connect import common, dbapi
from clickhouse_connect.cc_sqlalchemy import final
from clickhouse_connect.cc_sqlalchemy.datatypes.sqltypes import (
    UUID,
    AggregateFunction,
    Array,
    Boolean,
    DateTime,
    DateTime64,
    Decimal,
    Enum16,
    FixedString,
    Float64,
    Int8,
    IPv4,
    LowCardinality,
    Nullable,
    QBit,
    String,
    UInt16,
    UInt32,
    UInt64,
)
from clickhouse_connect.cc_sqlalchemy.ddl.custom import CreateDatabase, DropDatabase
from clickhouse_connect.cc_sqlalchemy.ddl.tableengine import MergeTree, ReplacingMergeTree, SummingMergeTree, engine_map
from clickhouse_connect.cc_sqlalchemy.dialect import ClickHouseDialect
from clickhouse_connect.driver.binding import quote_identifier
from tests.integration_tests.conftest import TestConfig


def test_create_database(test_engine: Engine, test_config: TestConfig, test_db: str):
    if test_db:
        common.set_setting("invalid_setting_action", "drop")
        with test_engine.begin() as conn:
            create_db = f"create_db_{test_db}"
            if not test_engine.dialect.has_database(conn, create_db):
                if test_config.host == "localhost":
                    conn.execute(CreateDatabase(create_db, "Atomic", exists_ok=True))
                else:
                    conn.execute(CreateDatabase(create_db, exists_ok=True))
            conn.execute(DropDatabase(create_db, missing_ok=True))


class ColorEnum(PyEnum):
    RED = 1
    BLUE = 2
    TEAL = -4
    COBALT = 877


def test_create_table(test_engine: Engine, test_db: str, test_table_engine: str):
    common.set_setting("invalid_setting_action", "drop")
    with test_engine.begin() as conn:
        table_cls = engine_map[test_table_engine]
        metadata = db.MetaData(schema=test_db)
        conn.execute(text("DROP TABLE IF EXISTS simple_table_test"))
        bool_type = Boolean
        date_tz64_type = DateTime64(3, "Europe/Moscow")
        table = db.Table(
            "simple_table_test",
            metadata,
            db.Column("key_col", Int8),
            db.Column("uint_col", UInt16),
            db.Column("dec_col", Decimal(38, 5)),  # Decimal128(5)
            db.Column("enum_col", Enum16(ColorEnum)),
            db.Column("float_col", Float64),
            db.Column("str_col", String),
            db.Column("fstr_col", FixedString(17)),
            db.Column("bool_col", bool_type),
            table_cls(("key_col", "uint_col"), primary_key="key_col"),
        )
        table.create(conn)
        conn.execute(text("DROP TABLE IF EXISTS advanced_table_test"))
        table = db.Table(
            "advanced_table_test",
            metadata,
            db.Column("key_col", UInt64),
            db.Column("uuid_col", UUID),
            db.Column("dt_col", DateTime),
            db.Column("ip_col", IPv4),
            db.Column("dt64_col", date_tz64_type),
            db.Column("lc_col", LowCardinality(FixedString(16))),
            db.Column("lc_date_col", LowCardinality(Nullable(String))),
            db.Column("null_dt_col", Nullable(DateTime("America/Denver"))),
            db.Column("arr_col", Array(UUID)),
            db.Column("agg_col", AggregateFunction("uniq", LowCardinality(String))),
            table_cls("key_col"),
        )
        table.create(conn)


def test_summing_merge_tree_columns_round_trip(test_engine: Engine, test_db: str):
    with test_engine.begin() as conn:
        metadata = MetaData(schema=test_db)
        table = db.Table(
            "summing_columns_test",
            metadata,
            db.Column("id", UInt32),
            db.Column("delta", UInt64),
            db.Column("n_tx", UInt64),
            SummingMergeTree(order_by="id", columns=("delta", "n_tx")),
        )
        table.drop(conn, checkfirst=True)
        try:
            table.create(conn)
            reflected = db.Table("summing_columns_test", MetaData(), schema=test_db, autoload_with=conn)
            assert isinstance(reflected.engine, SummingMergeTree)
            assert "columns='(delta, n_tx)'" in repr(reflected.engine)
        finally:
            table.drop(conn, checkfirst=True)


def test_declarative(test_engine: Engine, test_db: str, test_table_engine: str):
    common.set_setting("invalid_setting_action", "drop")
    with test_engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS users_test"))
        table_cls = engine_map[test_table_engine]
        base_cls = declarative_base(metadata=MetaData(schema=test_db))

        class User(base_cls):
            __tablename__ = "users_test"
            __table_args__ = (table_cls(order_by=["id", "name"]),)
            id = db.Column(UInt32, primary_key=True)
            name = db.Column(String)
            fullname = db.Column(String)
            nickname = db.Column(String)

        base_cls.metadata.create_all(test_engine)
        user = User(name="Alice")
        assert user.name == "Alice"


def test_final_modifier_replacing_merge_tree(test_engine: Engine, test_db: str):
    common.set_setting("invalid_setting_action", "drop")
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


def test_expression_sorting_key(test_engine: Engine, test_db: str):
    """Create a MergeTree with a function-expression sorting key and confirm the server reflects it."""
    common.set_setting("invalid_setting_action", "drop")
    with test_engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS expr_sorting_key_test"))
        metadata = MetaData(schema=test_db)
        genre = db.Column("genre", String)
        score = db.Column("score", UInt32)
        book_id = db.Column("book_id", UInt64)
        author_id = db.Column("author_id", UInt64)
        table = db.Table(
            "expr_sorting_key_test",
            metadata,
            genre,
            score,
            book_id,
            author_id,
            MergeTree(order_by=[genre, score, db.func.cityHash64(book_id, author_id)]),
        )
        table.create(conn)

        create_sql = conn.execute(text("SHOW CREATE TABLE expr_sorting_key_test")).scalar()
        assert "cityHash64" in create_sql

        conn.execute(text("DROP TABLE IF EXISTS expr_sorting_key_test"))


def test_desc_sorting_key(test_engine: Engine, test_db: str):
    """DESC sorting keys are stable from ClickHouse 26.6; verify end-to-end where supported."""
    common.set_setting("invalid_setting_action", "drop")
    with test_engine.begin() as conn:
        if not conn.connection.driver_connection.client.min_version("26.6"):
            pytest.skip("DESC sorting keys are experimental before ClickHouse 26.6")
        conn.execute(text("DROP TABLE IF EXISTS desc_sorting_key_test"))
        metadata = MetaData(schema=test_db)
        book_id = db.Column("book_id", UInt64)
        score = db.Column("score", UInt32)
        table = db.Table(
            "desc_sorting_key_test",
            metadata,
            book_id,
            score,
            MergeTree(order_by=[book_id, score.desc()]),
        )
        table.create(conn)

        create_sql = conn.execute(text("SHOW CREATE TABLE desc_sorting_key_test")).scalar()
        assert "DESC" in create_sql

        conn.execute(text("DROP TABLE IF EXISTS desc_sorting_key_test"))


def test_engine_clause_string_literal_roundtrip(test_engine: Engine, test_db: str):
    """A backslash string literal in an engine clause must use ClickHouse escaping so the server accepts it."""
    common.set_setting("invalid_setting_action", "drop")
    with test_engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS engine_literal_test"))
        metadata = MetaData(schema=test_db)
        table = db.Table(
            "engine_literal_test",
            metadata,
            db.Column("book_id", UInt64),
            db.Column("category", String),
            # SQLAlchemy's generic escaping would render 'a\' and the server would reject the DDL.
            MergeTree(order_by="book_id", partition_by=(db.column("category") == "a\\")),
        )
        table.create(conn)

        create_sql = conn.execute(text("SHOW CREATE TABLE engine_literal_test")).scalar()
        assert "a\\\\" in create_sql

        conn.execute(text("DROP TABLE IF EXISTS engine_literal_test"))


def test_qbit_table(test_engine: Engine, test_db: str, test_table_engine: str, test_config: TestConfig):
    """Test QBit type DDL and basic operations"""
    common.set_setting("invalid_setting_action", "drop")
    with test_engine.begin() as conn:
        if test_config.cloud:
            pytest.skip("QBit type requires allow_experimental_qbit_type setting, but settings are locked in cloud")

        if not conn.connection.driver_connection.client.min_version("25.10"):
            pytest.skip("QBit type requires ClickHouse version 25.10+")

        conn.execute(text("SET allow_experimental_qbit_type = 1"))

        table_cls = engine_map[test_table_engine]
        metadata = MetaData(schema=test_db)
        conn.execute(text("DROP TABLE IF EXISTS qbit_test"))

        table = db.Table(
            "qbit_test",
            metadata,
            db.Column("id", UInt32),
            db.Column("vector", QBit("Float32", 8)),
            db.Column("embedding", QBit("Float32", 128)),
            table_cls("id"),
        )
        table.create(conn)

        result = conn.execute(text("SHOW CREATE TABLE qbit_test"))
        create_sql = result.fetchone()[0]
        assert "QBit(Float32, 8)" in create_sql
        assert "QBit(Float32, 128)" in create_sql

        conn.execute(text("DROP TABLE IF EXISTS qbit_test"))


def test_comment_and_default_literal_roundtrip(param_client, call, test_db: str, client_mode: str):
    table_name = f"ddl_literal_{client_mode}_{uuid4().hex[:8]}"
    full_table = f"{quote_identifier(test_db)}.{quote_identifier(table_name)}"
    table_comment = "table\\'comment% adjacent%%"
    column_comment = "column trailing\\"
    default_value = "default\\path'part%"
    table = db.Table(
        table_name,
        MetaData(schema=test_db),
        db.Column("key", UInt64),
        db.Column("category", String, comment=column_comment, server_default=default_value),
        MergeTree(order_by="key"),
        comment=table_comment,
    )
    create_sql = str(
        db.schema.CreateTable(table).compile(
            dialect=ClickHouseDialect(dbapi=dbapi, server_side_params=True),
        )
    )
    call(param_client.command, f"DROP TABLE IF EXISTS {full_table}")

    try:
        call(param_client.command, create_sql)
        call(param_client.command, f"INSERT INTO {full_table} (key) VALUES (13)")

        table_rows = call(
            param_client.query,
            "SELECT comment FROM system.tables WHERE database = {database:String} AND name = {table:String}",
            parameters={"database": test_db, "table": table_name},
        ).result_rows
        column_rows = call(
            param_client.query,
            "SELECT comment FROM system.columns WHERE database = {database:String} AND table = {table:String} AND name = 'category'",
            parameters={"database": test_db, "table": table_name},
        ).result_rows
        default_rows = call(param_client.query, f"SELECT category FROM {full_table} WHERE key = 13").result_rows

        assert table_rows == [(table_comment,)]
        assert column_rows == [(column_comment,)]
        assert default_rows == [(default_value,)]
    finally:
        call(param_client.command, f"DROP TABLE IF EXISTS {full_table}")


def test_comment_and_default_literal_engine_roundtrip(test_engine: Engine, test_db: str):
    table_name = f"ddl_engine_literal_{uuid4().hex[:8]}"
    full_table = f"{quote_identifier(test_db)}.{quote_identifier(table_name)}"
    table_comment = "table\\'comment% adjacent%%"
    column_comment = "column\\'comment% adjacent%%"
    default_value = "default\\'value% adjacent%%"
    table = db.Table(
        table_name,
        MetaData(schema=test_db),
        db.Column("key", UInt64),
        db.Column("category", String, comment=column_comment, server_default=default_value),
        MergeTree(order_by="key"),
        comment=table_comment,
    )

    with test_engine.begin() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS {full_table}"))
        try:
            table.create(conn)
            conn.execute(text(f"INSERT INTO {full_table} (key) VALUES (13)"))

            stored_table_comment = conn.execute(
                text("SELECT comment FROM system.tables WHERE database = :database AND name = :table"),
                {"database": test_db, "table": table_name},
            ).scalar_one()
            stored_column_comment = conn.execute(
                text("SELECT comment FROM system.columns WHERE database = :database AND table = :table AND name = 'category'"),
                {"database": test_db, "table": table_name},
            ).scalar_one()
            stored_default = conn.execute(text(f"SELECT category FROM {full_table} WHERE key = 13")).scalar_one()

            assert stored_table_comment == table_comment
            assert stored_column_comment == column_comment
            assert stored_default == default_value
        finally:
            conn.execute(text(f"DROP TABLE IF EXISTS {full_table}"))
