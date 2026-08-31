from collections.abc import Callable
from unittest.mock import patch

import pytest
import sqlalchemy as db
from pytest import fixture
from sqlalchemy import MetaData, text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, declarative_base

from clickhouse_connect.cc_sqlalchemy.datatypes.sqltypes import FixedString, LowCardinality, String, UInt64
from clickhouse_connect.cc_sqlalchemy.ddl.tableengine import engine_map
from clickhouse_connect.cc_sqlalchemy.dialect import ClickHouseDialect


@fixture(scope="module", autouse=True, name="test_model")
def test_model_fixture(test_engine: Engine, test_db: str, test_table_engine: str):
    engine_cls = engine_map[test_table_engine]

    Base = declarative_base(metadata=MetaData(schema=test_db))  # noqa: N806

    class Model(Base):
        __tablename__ = "insert_model"
        __table_args__ = (engine_cls(order_by=["test_name", "value_1"]),)
        test_name = db.Column(LowCardinality(String), primary_key=True)
        value_1 = db.Column(String)
        metric_2 = db.Column(UInt64)
        description = db.Column(String)

    with test_engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS insert_model"))
        Base.metadata.create_all(test_engine)
        yield Model


def test_single_insert(test_engine: Engine, test_model):
    with test_engine.begin() as conn:
        conn.execute(
            db.insert(test_model).values(test_name="single_insert", value_1="v1", metric_2=25738, description="Single Desc"),
        )
        conn.execute(db.insert(test_model), {"test_name": "another_single_insert"})


def test_user_agent_integration_tags(test_engine: Engine):
    with test_engine.begin():
        client = test_engine.raw_connection().driver_connection.client
        ua = client.headers["User-Agent"]
        if db.__version__.startswith("1."):
            assert "sqlalchemy/1." in ua
        else:
            assert "sqlalchemy/2." in ua


def test_multiple_insert(test_engine: Engine, test_model):
    session = Session(test_engine)
    model_1 = test_model(
        test_name="multi_1",
        value_1="v1",
        metric_2=100,
        description="First of Many",
    )
    model_2 = test_model(
        test_name="multi_2",
        value_1="v2",
        metric_2=100,
        description="Second of Many",
    )
    model_3 = test_model(
        value_1="v7",
        metric_2=77,
        description="Third of Many",
        test_name="odd_one",
    )
    session.add(model_1)
    session.add(model_2)
    session.add(model_3)
    session.commit()


def test_bytes_insert(test_engine: Engine, test_db: str, test_table_engine: str):
    engine_cls = engine_map[test_table_engine]
    Base = declarative_base(metadata=MetaData(schema=test_db))  # noqa: N806

    class BytesModel(Base):
        __tablename__ = "bytes_insert_model"
        __table_args__ = (engine_cls(order_by=["id"]),)
        id = db.Column(FixedString(12), primary_key=True)
        data = db.Column(String)

    with test_engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS bytes_insert_model"))
        Base.metadata.create_all(test_engine)

    fixed = b"j!lUA\xf8\x93q;ky\x00"  # 12 bytes: non-UTF8 and a null byte
    session = Session(test_engine)
    session.add(BytesModel(id=fixed, data=b"hello"))
    session.commit()

    with test_engine.begin() as conn:
        row = conn.execute(text("SELECT id, data FROM bytes_insert_model")).one()
    assert row[0] == fixed
    assert row[1] == "hello"


def test_bulk_insert(test_engine: Engine, test_model):
    session = Session(test_engine)
    model_1 = test_model(
        test_name="bulk_1",
        value_1="v1",
        metric_2=100,
        description="First of Bulk",
    )
    model_2 = test_model(
        test_name="bulk_2",
        value_1="v2",
        metric_2=100,
        description="Second of Bulk",
    )
    model_3 = test_model(
        value_1="vb78",
        metric_2=528,
        description="Third of Bulk",
        test_name="bulk",
    )
    session.bulk_save_objects([model_1, model_2, model_3])
    session.commit()


def test_compiled_executemany_uses_native_insert(test_engine: Engine, test_model):
    rows = [
        {"test_name": "native_1", "value_1": "v1", "metric_2": 13, "description": "Native one"},
        {"description": "Native two", "metric_2": 79, "value_1": "v2", "test_name": "native_2"},
    ]
    with test_engine.begin() as conn:
        client = conn.connection.driver_connection.client
        with patch.object(client, "insert", wraps=client.insert) as native_insert:
            result = conn.execute(db.insert(test_model), rows)

        assert native_insert.call_count == 1
        assert result.rowcount == 2


def test_compiled_executemany_uses_escaped_bind_name(
    test_engine: Engine,
    table_context: Callable,
    test_db: str,
):
    with table_context("sqlalchemy_escaped_bind", ["value%pct UInt64"]):
        table = db.Table(
            "sqlalchemy_escaped_bind",
            MetaData(schema=test_db),
            db.Column("value%pct", UInt64),
        )
        with test_engine.begin() as conn:
            client = conn.connection.driver_connection.client
            with patch.object(client, "insert", wraps=client.insert) as native_insert:
                result = conn.execute(db.insert(table), [{"value%pct": 13}, {"value%pct": 79}])

            assert native_insert.call_count == 1
            assert result.rowcount == 2
            assert conn.execute(db.select(table.c["value%pct"]).order_by(table.c["value%pct"])).all() == [(13,), (79,)]


@pytest.mark.skipif(db.__version__.startswith("1."), reason="ORM bulk INSERT is a SQLAlchemy 2.x path")
def test_joined_table_bulk_insert_uses_effective_statement_table(
    test_engine: Engine,
    table_context: Callable,
    test_db: str,
):
    parent_table_name = "sqlalchemy_jti_parent"
    child_table_name = "sqlalchemy_jti_child"
    with (
        table_context(parent_table_name, ["id UInt64", "kind String", "parent_value String"]),
        table_context(child_table_name, ["id UInt64", "child_value String"]),
    ):
        base = declarative_base(metadata=MetaData(schema=test_db))

        class Parent(base):
            __tablename__ = parent_table_name
            id = db.Column(UInt64, primary_key=True)
            kind = db.Column(String)
            parent_value = db.Column(String)
            __mapper_args__ = {"polymorphic_on": kind, "polymorphic_identity": "parent"}

        class Child(Parent):
            __tablename__ = child_table_name
            id = db.Column(UInt64, db.ForeignKey(f"{test_db}.{parent_table_name}.id"), primary_key=True)
            child_value = db.Column(String)
            __mapper_args__ = {"polymorphic_identity": "child"}

        observed_plans = []
        original_planner = ClickHouseDialect._ch_native_insert_plan

        def capture_plan(context, settings):
            plan = original_planner(context, settings)
            observed_plans.append(
                (
                    context.compiled.statement.table.name,
                    context.compiled.compile_state.statement.table.name,
                    plan,
                )
            )
            return plan

        with patch.object(ClickHouseDialect, "_ch_native_insert_plan", staticmethod(capture_plan)):
            with Session(test_engine) as session:
                session.execute(
                    db.insert(Child),
                    [
                        {"id": 13, "kind": "child", "parent_value": "parent_13", "child_value": "child_13"},
                        {"id": 79, "kind": "child", "parent_value": "parent_79", "child_value": "child_79"},
                    ],
                )
                session.commit()

        parent_plan = next(item for item in observed_plans if item[1] == parent_table_name)
        assert parent_plan[0] == child_table_name
        assert parent_plan[2] is not None
        assert parent_plan[2].table == f"`{test_db}`.`{parent_table_name}`"
        with test_engine.connect() as conn:
            assert conn.execute(db.select(Parent.id, Parent.parent_value).order_by(Parent.id)).all() == [
                (13, "parent_13"),
                (79, "parent_79"),
            ]
            assert conn.execute(db.select(Child.__table__.c.id, Child.child_value).order_by(Child.__table__.c.id)).all() == [
                (13, "child_13"),
                (79, "child_79"),
            ]


def test_compiled_expression_executemany_preserves_sql(test_engine: Engine, test_model):
    statement = db.insert(test_model).values(
        test_name=db.bindparam("test_name"),
        value_1=db.func.hex(db.bindparam("raw_value")),
        metric_2=db.bindparam("metric_2"),
        description=db.bindparam("description"),
    )
    rows = [
        {"test_name": "expr_1", "raw_value": "user_1", "metric_2": 13, "description": "Expression one"},
        {"description": "Expression two", "metric_2": 79, "raw_value": "user_2", "test_name": "expr_2"},
    ]
    with test_engine.begin() as conn:
        client = conn.connection.driver_connection.client
        with patch.object(client, "insert", wraps=client.insert) as native_insert:
            result = conn.execute(statement, rows)

        assert native_insert.call_count == 0
        assert result.rowcount == 2
        stored = conn.execute(
            db.select(test_model.test_name, test_model.value_1)
            .where(test_model.test_name.in_(["expr_1", "expr_2"]))
            .order_by(test_model.test_name)
        ).all()
        assert stored == [("expr_1", "757365725F31"), ("expr_2", "757365725F32")]


def test_compiled_expression_executemany_preserves_percent_encoding(
    test_engine: Engine,
    table_context: Callable,
    test_db: str,
):
    table_name = "sqlalchemy%expression_insert"
    with table_context(table_name, ["value%pct String", "nonce String"]):
        table = db.Table(
            table_name,
            MetaData(schema=test_db),
            db.Column("value%pct", String),
            db.Column("nonce", String),
        )
        statement = db.insert(table).values(
            {
                table.c["value%pct"]: db.literal_column("'single% adjacent%% tail%'"),
                table.c.nonce: db.literal_column("toString(generateUUIDv4())"),
            }
        )
        with test_engine.begin() as conn:
            client = conn.connection.driver_connection.client
            with patch.object(client, "insert", wraps=client.insert) as native_insert:
                result = conn.execute(statement, [{}, {}])

            assert native_insert.call_count == 0
            assert result.rowcount == 2
            stored = conn.execute(db.select(table.c["value%pct"], table.c.nonce)).all()
            assert [row[0] for row in stored] == ["single% adjacent%% tail%", "single% adjacent%% tail%"]
            assert len({row[1] for row in stored}) == 2


def test_compiled_bind_expression_executemany_preserves_sql(test_engine: Engine, test_model, test_db: str):
    class HexString(db.TypeDecorator):
        impl = String
        cache_ok = True

        def bind_expression(self, bindvalue):
            return db.func.hex(bindvalue)

    table = db.Table(
        "insert_model",
        MetaData(schema=test_db),
        db.Column("test_name", String),
        db.Column("value_1", HexString()),
        db.Column("metric_2", UInt64),
        db.Column("description", String),
    )
    rows = [
        {"test_name": "bind_expr_1", "value_1": "user_1", "metric_2": 13, "description": "Bind one"},
        {"test_name": "bind_expr_2", "value_1": "user_2", "metric_2": 79, "description": "Bind two"},
    ]
    with test_engine.begin() as conn:
        client = conn.connection.driver_connection.client
        with patch.object(client, "insert", wraps=client.insert) as native_insert:
            result = conn.execute(db.insert(table), rows)

        assert native_insert.call_count == 0
        assert result.rowcount == 2
        stored = conn.execute(
            db.select(test_model.test_name, test_model.value_1)
            .where(test_model.test_name.in_(["bind_expr_1", "bind_expr_2"]))
            .order_by(test_model.test_name)
        ).all()
        assert stored == [("bind_expr_1", "757365725F31"), ("bind_expr_2", "757365725F32")]


def test_exec_driver_sql_executemany_preserves_sql(test_engine: Engine, test_model):
    operation = (
        "INSERT INTO insert_model (test_name, value_1, metric_2, description) "
        "VALUES (%(test_name)s, hex(%(raw_value)s), %(metric_2)s, %(description)s)"
    )
    rows = [
        {"test_name": "driver_1", "raw_value": "user_1", "metric_2": 13, "description": "Driver one"},
        {"description": "Driver two", "metric_2": 79, "raw_value": "user_2", "test_name": "driver_2"},
    ]
    with test_engine.begin() as conn:
        client = conn.connection.driver_connection.client
        with patch.object(client, "insert", wraps=client.insert) as native_insert:
            result = conn.exec_driver_sql(operation, rows)

        assert native_insert.call_count == 0
        assert result.rowcount == 2
        stored = conn.execute(
            db.select(test_model.test_name, test_model.value_1)
            .where(test_model.test_name.in_(["driver_1", "driver_2"]))
            .order_by(test_model.test_name)
        ).all()
        assert stored == [("driver_1", "757365725F31"), ("driver_2", "757365725F32")]
