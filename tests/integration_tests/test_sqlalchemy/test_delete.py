import pytest
import sqlalchemy as db
from sqlalchemy import MetaData, delete, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import CompileError

from clickhouse_connect.cc_sqlalchemy.datatypes.sqltypes import String, UInt64
from clickhouse_connect.cc_sqlalchemy.ddl.tableengine import engine_map


def test_delete_with_table_object(test_engine: Engine, test_db: str, test_table_engine: str):
    """DELETE using SQLAlchemy Table object"""
    engine_cls = engine_map[test_table_engine]
    metadata = MetaData(schema=test_db)

    test_table = db.Table(
        "delete_test",
        metadata,
        db.Column("id", UInt64),
        db.Column("name", String),
        db.Column("status", String),
        engine_cls("id"),
    )

    with test_engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS delete_test"))
        test_table.create(conn)

        conn.execute(db.insert(test_table).values({"id": 1, "name": "hello world", "status": "active"}))
        conn.execute(db.insert(test_table).values({"id": 2, "name": "test data", "status": "inactive"}))
        conn.execute(db.insert(test_table).values({"id": 3, "name": "hello test", "status": "active"}))
        starting = conn.execute(db.select(test_table).order_by(test_table.c.id)).fetchall()
        assert len(starting) == 3
        assert [row.id for row in starting] == [1, 2, 3]

        delete_stmt = delete(test_table).where(test_table.c.name.like("%hello%"))
        conn.execute(delete_stmt)

        remaining = conn.execute(db.select(test_table).order_by(test_table.c.id)).fetchall()
        assert len(remaining) == 1
        assert remaining[0].name == "test data"


def test_delete_with_multiple_conditions(test_engine: Engine, test_db: str, test_table_engine: str):
    """DELETE with multiple WHERE conditions"""
    engine_cls = engine_map[test_table_engine]
    metadata = MetaData(schema=test_db)

    test_table = db.Table(
        "delete_multi_test",
        metadata,
        db.Column("id", UInt64),
        db.Column("category", String),
        db.Column("value", UInt64),
        engine_cls("id"),
    )

    with test_engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS delete_multi_test"))
        test_table.create(conn)

        conn.execute(db.insert(test_table).values({"id": 1, "category": "A", "value": 100}))
        conn.execute(db.insert(test_table).values({"id": 2, "category": "B", "value": 200}))
        conn.execute(db.insert(test_table).values({"id": 3, "category": "A", "value": 300}))
        conn.execute(db.insert(test_table).values({"id": 4, "category": "C", "value": 50}))
        starting = conn.execute(db.select(test_table).order_by(test_table.c.id)).fetchall()
        assert len(starting) == 4
        assert [row.id for row in starting] == [1, 2, 3, 4]

        delete_stmt = delete(test_table).where((test_table.c.category == "A") & (test_table.c.value > 150))
        conn.execute(delete_stmt)

        remaining = conn.execute(db.select(test_table).order_by(test_table.c.id)).fetchall()
        assert len(remaining) == 3
        assert [row.id for row in remaining] == [1, 2, 4]


def test_delete_all_rows_error(test_engine: Engine, test_db: str, test_table_engine: str):
    """DELETE without WHERE should raise"""
    engine_cls = engine_map[test_table_engine]
    metadata = MetaData(schema=test_db)

    test_table = db.Table(
        "delete_all_test",
        metadata,
        db.Column("id", UInt64),
        db.Column("data", String),
        engine_cls("id"),
    )

    with test_engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS delete_all_test"))
        test_table.create(conn)
        conn.execute(db.insert(test_table).values({"id": 1, "data": "test1"}))

        delete_stmt = delete(test_table)
        with pytest.raises(CompileError, match="require a WHERE clause"):
            conn.execute(delete_stmt)


def test_delete_basic_functionality(test_engine: Engine, test_table_engine: str):
    """Basic DELETE statement compilation and execution"""
    engine_cls = engine_map[test_table_engine]
    metadata = MetaData()

    test_table = db.Table(
        "delete_simple_test",
        metadata,
        db.Column("id", UInt64),
        db.Column("name", String),
        engine_cls("id"),
    )

    with test_engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS delete_simple_test"))
        test_table.create(conn)

        conn.execute(db.insert(test_table).values({"id": 1, "name": "test_row"}))
        starting = conn.execute(db.select(test_table).order_by(test_table.c.id)).fetchall()
        assert len(starting) == 1
        assert starting[0].name == "test_row"

        delete_stmt = delete(test_table).where(test_table.c.id == 1)
        conn.execute(delete_stmt)

        result = conn.execute(db.select(test_table).order_by(test_table.c.id)).fetchall()
        assert len(result) == 0


def test_delete_with_text_condition(test_engine: Engine, test_table_engine: str):
    """Test DELETE with text-based WHERE condition"""
    engine_cls = engine_map[test_table_engine]
    metadata = MetaData()

    test_table = db.Table(
        "delete_text_test",
        metadata,
        db.Column("id", UInt64),
        db.Column("status", String),
        engine_cls("id"),
    )

    with test_engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS delete_text_test"))
        test_table.create(conn)

        conn.execute(db.insert(test_table).values({"id": 1, "status": "active"}))
        conn.execute(db.insert(test_table).values({"id": 2, "status": "inactive"}))
        conn.execute(db.insert(test_table).values({"id": 3, "status": "active"}))
        starting = conn.execute(db.select(test_table).order_by(test_table.c.id)).fetchall()
        assert len(starting) == 3
        assert [row.id for row in starting] == [1, 2, 3]

        delete_stmt = delete(test_table).where(test_table.c.status == "inactive")
        result = conn.execute(delete_stmt)

        result = conn.execute(db.select(test_table).order_by(test_table.c.id)).fetchall()
        assert len(result) == 2
        assert [row.id for row in result] == [1, 3]


def test_explicit_delete(test_engine: Engine, test_table_engine: str):
    """Test explicit DELETE"""
    engine_cls = engine_map[test_table_engine]
    metadata = MetaData()

    test_table = db.Table(
        "delete_explicit_test",
        metadata,
        db.Column("id", UInt64),
        db.Column("name", String),
        engine_cls("id"),
    )

    with test_engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS delete_explicit_test"))
        test_table.create(conn)

        conn.execute(db.insert(test_table).values({"id": 1, "name": "hello world"}))
        conn.execute(db.insert(test_table).values({"id": 2, "name": "test data"}))
        conn.execute(db.insert(test_table).values({"id": 3, "name": "hello test"}))
        starting = conn.execute(db.select(test_table).order_by(test_table.c.id)).fetchall()
        assert len(starting) == 3
        assert [row.id for row in starting] == [1, 2, 3]

        conn.execute(text("DELETE FROM delete_explicit_test WHERE name LIKE '%hello%'"))

        result = conn.execute(db.select(test_table).order_by(test_table.c.id)).fetchall()
        assert len(result) == 1
        assert result[0].name == "test data"
