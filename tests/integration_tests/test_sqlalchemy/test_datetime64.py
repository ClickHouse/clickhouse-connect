from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import pytest
import sqlalchemy as sa
from sqlalchemy.orm import Session, declarative_base

from clickhouse_connect import dbapi
from clickhouse_connect.cc_sqlalchemy.datatypes.base import sqla_type_from_name
from clickhouse_connect.cc_sqlalchemy.datatypes.sqltypes import Array, DateTime, DateTime64, Nullable, Tuple, UInt32
from clickhouse_connect.cc_sqlalchemy.dialect import ClickHouseDialect

STAMP = datetime(2024, 6, 13, 7, 8, 9, 123456, tzinfo=timezone.utc)
NAIVE_STAMP = STAMP.replace(tzinfo=None)


@pytest.fixture(params=[False, True], ids=["client-side", "server-side"])
def datetime_engine(request, test_engine):
    engine = sa.create_engine(test_engine.url, server_side_params=request.param)
    yield engine
    engine.dispose()


@pytest.mark.parametrize("precision", [0, 3, 6, 9])
@pytest.mark.parametrize("route", ["single", "native", "multivalues", "fallback", "orm"])
def test_datetime64_insert_precision(datetime_engine, table_context, test_db, precision, route):
    table_name = "sqlalchemy_datetime64_precision"
    with table_context(table_name, ["id UInt32", f"stamp DateTime64({precision}, 'UTC')", "seconds DateTime('UTC')"]):
        table = sa.Table(
            table_name,
            sa.MetaData(schema=test_db),
            sa.Column("id", UInt32, primary_key=True),
            sa.Column("stamp", DateTime64(precision, "UTC")),
            sa.Column("seconds", DateTime("UTC")),
        )
        rows = [{"id": key, "stamp": STAMP, "seconds": STAMP} for key in (13, 79)]
        with datetime_engine.begin() as conn:
            client = conn.connection.driver_connection.client
            with patch.object(client, "insert", wraps=client.insert) as native_insert:
                if route == "single":
                    for row in rows:
                        conn.execute(table.insert().values(**row))
                elif route == "multivalues":
                    conn.execute(table.insert().values(rows))
                elif route == "fallback":
                    conn.execute(table.insert().values(id=sa.bindparam("id", type_=UInt32()) + 0), rows)
                elif route == "orm":
                    base = declarative_base()

                    class Event(base):
                        __table__ = table

                    with Session(bind=conn) as session:
                        for row in rows:
                            session.add(Event(**row))
                            session.flush()
                else:
                    conn.execute(table.insert(), rows)
                assert native_insert.call_count == (1 if route == "native" else 0)

            divisor = 10 ** max(6 - precision, 0)
            expected = NAIVE_STAMP.replace(microsecond=STAMP.microsecond // divisor * divisor)
            actual = conn.execute(sa.select(table).order_by(table.c.id)).all()
            assert actual == [(key, expected, NAIVE_STAMP.replace(microsecond=0)) for key in (13, 79)]
            assert conn.execute(sa.select(table.c.id).where(table.c.stamp == expected).order_by(table.c.id)).all() == [(13,), (79,)]
            assert conn.execute(sa.select(table.c.id).where(table.c.stamp == expected + timedelta(seconds=1))).all() == []


@pytest.mark.parametrize(
    ("type_", "value", "expected"),
    [
        (DateTime64(6, "UTC"), STAMP, NAIVE_STAMP),
        (Array(DateTime64(6, "UTC")), [STAMP], [NAIVE_STAMP]),
        (Tuple(DateTime64(6, "UTC"), DateTime("UTC")), (STAMP, STAMP), (NAIVE_STAMP, NAIVE_STAMP.replace(microsecond=0))),
        (Array(Tuple(UInt32, DateTime64(6, "UTC"))), [(13, STAMP)], [(13, NAIVE_STAMP)]),
        (Nullable(DateTime64(6, "UTC")), STAMP, NAIVE_STAMP),
        (Array(Nullable(DateTime64(6, "UTC"))), [None, STAMP], [None, NAIVE_STAMP]),
        (sqla_type_from_name("datetime64(6, 'UTC')"), STAMP, NAIVE_STAMP),
        (sqla_type_from_name("DaTeTiMe64(6, 'UTC')"), STAMP, NAIVE_STAMP),
        (Nullable(DateTime64(6, "UTC")), None, None),
    ],
    ids=["scalar", "array", "mixed-tuple", "array-tuple", "nullable", "array-nullable", "lowercase", "mixed-case", "null"],
)
def test_datetime64_typed_shapes(datetime_engine, type_, value, expected):
    statement = sa.select(sa.cast(sa.bindparam("stamp", value, type_=type_), type_))
    with datetime_engine.connect() as conn:
        assert conn.execute(statement).scalar_one() == expected
        compiled = str(statement.compile(datetime_engine, compile_kwargs={"literal_binds": True}))
        assert conn.exec_driver_sql(compiled).scalar_one() == expected


def test_datetime64_expanding_and_literal_execute(datetime_engine):
    value = sa.cast(sa.literal(STAMP, DateTime64(6, "UTC")), DateTime64(6, "UTC"))
    with datetime_engine.connect() as conn:
        assert conn.execute(sa.select(value.in_([STAMP, STAMP + timedelta(seconds=1)]))).scalar_one() == 1
        assert conn.execute(sa.select(value.in_([STAMP + timedelta(seconds=1)]))).scalar_one() == 0
        assert conn.execute(sa.select(value.in_([]))).scalar_one() == 0
        assert conn.execute(sa.select(sa.tuple_(sa.literal(13, UInt32()), value).in_([(79, STAMP), (13, STAMP)]))).scalar_one() == 1
        for name, literal_execute in [("stamp", False), ("stamp", True)]:
            statement = sa.select(
                sa.cast(sa.bindparam(name, STAMP, DateTime64(6, "UTC"), literal_execute=literal_execute), DateTime64(6, "UTC"))
            )
            assert conn.execute(statement).scalar_one() == NAIVE_STAMP


def test_datetime64_decorators_variants_and_escaped_names(test_engine):
    class AdjustedStamp(sa.TypeDecorator):
        impl = sa.DateTime
        cache_ok = True

        def load_dialect_impl(self, dialect):
            return DateTime64(6, "UTC")

        def process_bind_param(self, value, dialect):
            return value + timedelta(microseconds=79)

    class StringStamp(AdjustedStamp):
        cache_ok = True

        def process_bind_param(self, value, dialect):
            return value.strftime("%Y-%m-%d %H:%M:%S.%f")

    with test_engine.connect() as conn:
        for type_, expected in [
            (AdjustedStamp(), NAIVE_STAMP + timedelta(microseconds=79)),
            (StringStamp(), NAIVE_STAMP),
            (sa.DateTime().with_variant(DateTime64(6, "UTC"), "clickhousedb"), NAIVE_STAMP),
        ]:
            statement = sa.select(sa.cast(sa.bindparam("stamp%value", STAMP, type_), DateTime64(6, "UTC")))
            assert conn.execute(statement).scalar_one() == expected
            compiled = str(statement.compile(test_engine, compile_kwargs={"literal_binds": True}))
            assert conn.exec_driver_sql(compiled).scalar_one() == expected


@pytest.mark.parametrize("compiled_server_side", [False, True])
def test_datetime64_precompiled_parameter_mode(datetime_engine, compiled_server_side):
    statement = sa.select(sa.cast(sa.bindparam("stamp", STAMP, DateTime64(6, "UTC")), DateTime64(6, "UTC")))
    compiled = statement.compile(dialect=ClickHouseDialect(dbapi=dbapi, server_side_params=compiled_server_side))

    with datetime_engine.connect() as conn:
        assert conn.execute(compiled).scalar_one() == NAIVE_STAMP


@pytest.mark.parametrize("first_type", [DateTime("UTC"), DateTime64(3, "UTC"), DateTime64(6, "UTC")])
@pytest.mark.parametrize("executemany", [False, True])
def test_datetime64_shared_bind_compatibility(test_engine, table_context, test_db, first_type, executemany):
    with table_context("sqlalchemy_datetime64_shared", [f"first {first_type.name}", "second DateTime64(6, 'UTC')"]):
        table = sa.Table(
            "sqlalchemy_datetime64_shared",
            sa.MetaData(schema=test_db),
            sa.Column("first", first_type),
            sa.Column("second", DateTime64(6, "UTC")),
        )
        statement = table.insert().values(first=sa.bindparam("stamp"), second=sa.bindparam("stamp"))
        with test_engine.begin() as conn:
            parameters = {"stamp": STAMP}
            conn.execute(statement, [parameters, parameters] if executemany else parameters)
            first_expected = NAIVE_STAMP
            second_expected = NAIVE_STAMP
            if isinstance(first_type, DateTime):
                first_expected = second_expected = NAIVE_STAMP.replace(microsecond=0)
            elif first_type.ch_type.scale == 3:
                first_expected = NAIVE_STAMP.replace(microsecond=123000)
            assert conn.execute(sa.select(table)).all() == [(first_expected, second_expected)] * (2 if executemany else 1)


@pytest.mark.parametrize("tuple_bind", [False, True])
def test_datetime64_escaped_expanding_bind(test_engine, tuple_bind):
    column = sa.cast(sa.literal(STAMP, DateTime64(6, "UTC")), DateTime64(6, "UTC"))
    values = [STAMP, STAMP + timedelta(seconds=1)]
    if tuple_bind:
        column = sa.tuple_(column, sa.literal(13, UInt32()))
        values = [(value, 13) for value in values]
    statement = sa.select(column.in_(sa.bindparam("ts%val", expanding=True)))

    with test_engine.connect() as conn:
        assert conn.execute(statement, {"ts%val": values}).scalar_one() == 1
        assert conn.execute(statement, {"ts%val": values[1:]}).scalar_one() == 0
        assert conn.execute(statement, {"ts%val": []}).scalar_one() == 0


@pytest.mark.parametrize("dimensions", [None, 1, 2, 3])
@pytest.mark.parametrize("wrapped", [False, True])
def test_datetime64_generic_array_roundtrip(test_engine, dimensions, wrapped):
    class StampArray(sa.TypeDecorator):
        impl = sa.ARRAY(Nullable(DateTime64(6)), dimensions=dimensions)
        cache_ok = True

    bind_type = StampArray() if wrapped else sa.ARRAY(Nullable(DateTime64(6)), dimensions=dimensions)
    cast_type = Array(Nullable(DateTime64(6, "UTC")))
    value = [STAMP, None]
    expected = [NAIVE_STAMP, None]
    for _ in range((dimensions or 2) - 1):
        value = [value]
        expected = [expected]
        cast_type = Array(cast_type)
    statement = sa.select(sa.cast(sa.bindparam("stamps", value, bind_type), cast_type))

    with test_engine.connect() as conn:
        assert conn.execute(statement).scalar_one() == expected
        if not sa.__version__.startswith("1."):
            compiled = str(statement.compile(test_engine, compile_kwargs={"literal_binds": True}))
            assert conn.exec_driver_sql(compiled).scalar_one() == expected


@pytest.mark.parametrize("inferred_dimensions", [False, True])
@pytest.mark.parametrize("nested", [False, True])
def test_datetime64_generic_array_tuple_roundtrip(test_engine, inferred_dimensions, nested):
    item_type = Tuple(DateTime64(6, "UTC"), DateTime("UTC"), UInt32)
    bind_type = sa.ARRAY(item_type, dimensions=None if inferred_dimensions else 1 + nested)
    cast_type = Array(item_type)
    value = [(STAMP, STAMP, 13)]
    expected = [(NAIVE_STAMP, NAIVE_STAMP.replace(microsecond=0), 13)]
    if nested:
        value = [value]
        expected = [expected]
        cast_type = Array(cast_type)
    statement = sa.select(sa.cast(sa.bindparam("value", value, bind_type), cast_type))

    with test_engine.connect() as conn:
        assert conn.execute(statement).scalar_one() == expected
