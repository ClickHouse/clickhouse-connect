from datetime import datetime, timezone
from decimal import Decimal
from enum import Enum
from unittest.mock import patch

import pytest
import sqlalchemy as sa
from sqlalchemy.orm import Session, column_property, declarative_base

from clickhouse_connect.cc_sqlalchemy.datatypes.sqltypes import DateTime64, UInt64
from clickhouse_connect.cc_sqlalchemy.sql import cte

Base = declarative_base()


class Number(Base):
    __tablename__ = "numbers"
    __table_args__ = {"schema": "system"}

    number = sa.Column(UInt64, primary_key=True)


GenericBase = declarative_base()


class GenericNumber(GenericBase):
    __tablename__ = "numbers"
    __table_args__ = {"schema": "system"}

    number = sa.Column(sa.Integer, primary_key=True)
    number_label = column_property(sa.func.toString(number, type_=sa.String))


class _UtcDateTime(sa.TypeDecorator):
    impl = DateTime64(6, "UTC")
    cache_ok = True

    def process_result_value(self, value, dialect):
        return value.astimezone(timezone.utc) if value is not None else None


class _NestedUtcDateTime(_UtcDateTime):
    impl = _UtcDateTime
    cache_ok = True


class _Status(Enum):
    OPEN = "open"
    CLOSED = "closed"


@pytest.fixture
def metadata_connection(client_mode, call, test_engine):
    if client_mode == "sync":
        with test_engine.connect() as connection:
            yield connection
        return

    pytest.importorskip("sqlalchemy", minversion="2.0.44")
    from sqlalchemy.ext.asyncio import create_async_engine

    engine = create_async_engine(test_engine.url.set(drivername="clickhousedb+async"))
    connection = call(engine.connect)
    try:
        yield connection
    finally:
        call(connection.close)
        call(engine.dispose)


@pytest.mark.parametrize("no_parameters", [False, True])
def test_empty_core_result_keeps_keys_without_metadata_probe(metadata_connection, client_mode, call, no_parameters):
    connection = metadata_connection
    raw_connection = call(connection.get_raw_connection) if client_mode == "async" else connection.connection
    client = raw_connection.driver_connection
    if client_mode == "sync":
        client = client.client
    statement = (
        sa.select(
            Number.number.label("the_number"),
            sa.func.toString(Number.number).label("number_label"),
            Number.number + Number.number,
        )
        .where(sa.false())
        .execution_options(no_parameters=no_parameters)
    )

    with patch.object(client, "query", wraps=client.query) as query:
        result = call(connection.execute, statement)

    assert list(result.keys()) == ["the_number", "number_label", "anon_1"]
    assert result.all() == []
    assert query.call_count == 1


@pytest.mark.parametrize("empty", [True, False], ids=["empty", "populated"])
@pytest.mark.parametrize(
    "result_type, value, expected",
    [
        (
            _UtcDateTime(),
            sa.func.toDateTime64("2026-10-13 07:09:11.123456", 6, "UTC"),
            datetime(2026, 10, 13, 7, 9, 11, 123456, timezone.utc),
        ),
        (
            _NestedUtcDateTime(),
            sa.func.toDateTime64("2026-10-13 07:09:11.123456", 6, "UTC"),
            datetime(2026, 10, 13, 7, 9, 11, 123456, timezone.utc),
        ),
        (
            sa.Enum(_Status),
            sa.sql.functions.Function("CAST", "OPEN", "Enum8('OPEN' = 1, 'CLOSED' = 2)"),
            _Status.OPEN,
        ),
        (sa.Boolean(), sa.true(), True),
        (sa.Numeric(10, 2), sa.func.toDecimal64("13.79", 2), Decimal("13.79")),
    ],
)
def test_result_processors_skip_metadata_probe(metadata_connection, client_mode, call, empty, result_type, value, expected):
    if not empty and sa.__version__.startswith("1.4.") and isinstance(result_type, sa.Enum):
        pytest.skip("SQLAlchemy 1.4 Enum processors require bytes with this dialect's existing Unicode configuration")
    connection = metadata_connection
    raw_connection = call(connection.get_raw_connection) if client_mode == "async" else connection.connection
    client = raw_connection.driver_connection
    if client_mode == "sync":
        client = client.client
    expression = sa.func.if_(Number.number == 0, value, None, type_=result_type).label("value_1")
    statement = sa.select(expression).where(Number.number < 2).order_by(Number.number).limit(2)
    if empty:
        statement = statement.where(sa.false())

    with patch.object(client, "query", wraps=client.query) as query:
        result = call(connection.execute, statement)

    assert list(result.keys()) == ["value_1"]
    if empty:
        assert result.cursor.description[0][1] is None
        assert result.cursor.description[0][6] is None
    assert result.all() == ([] if empty else [(expected,), (None,)])
    assert query.call_count == 1


@pytest.mark.parametrize("entity", [Number, GenericNumber])
def test_empty_orm_result_keeps_entity_keys_without_metadata_probe(metadata_connection, client_mode, call, entity):
    connection = metadata_connection
    if client_mode == "sync":
        session = Session(connection)
        client = connection.connection.driver_connection.client
    else:
        from sqlalchemy.ext.asyncio import AsyncSession

        session = AsyncSession(connection)
        client = call(connection.get_raw_connection).driver_connection
    try:
        with patch.object(client, "query", wraps=client.query) as query:
            result = call(session.execute, sa.select(entity).where(sa.false()))

        assert list(result.keys()) == [entity.__name__]
        assert result.scalars().all() == []
        assert query.call_count == 1
    finally:
        call(session.close)


@pytest.mark.parametrize(
    "expression",
    [
        sa.literal_column("*"),
        sa.literal_column("COLUMNS('number')"),
        sa.func.untuple(sa.func.tuple(13, 79)),
    ],
)
def test_empty_expanding_result_preserves_server_keys(metadata_connection, call, expression):
    statement = sa.select(expression).select_from(Number.__table__).limit(1)
    populated = call(metadata_connection.execute, statement)
    expected_keys = list(populated.keys())

    empty = call(metadata_connection.execute, statement.where(sa.false()))

    assert list(empty.keys()) == expected_keys
    assert empty.all() == []


@pytest.mark.parametrize(
    "statement, expected_keys",
    [
        (
            sa.select(sa.column(sa.sql.quoted_name("number, 13 AS extra", False))).select_from(Number.__table__).where(sa.false()),
            ["number", "extra"],
        ),
        (
            sa.select(sa.column(sa.sql.quoted_name("COLUMNS('number')", False))).select_from(Number.__table__).where(sa.false()),
            ["COLUMNS('number')"],
        ),
        (
            sa.select(Number.number.label(sa.sql.quoted_name("value_1, 13 AS value_2", False))).where(sa.false()),
            ["value_1", "value_2"],
        ),
        (sa.select(Number.number).prefix_with("COLUMNS('number'),").where(sa.false()), ["number", "number"]),
        (
            sa.select(Number.number)
            .prefix_with("COLUMNS('number'),")
            .where(sa.false())
            .union_all(sa.select(Number.number).prefix_with("COLUMNS('number'),").where(sa.false())),
            ["number", "number"],
        ),
        (
            sa.select(sa.literal(13).label("value_1")).suffix_with(", 79 AS value_2 WHERE false"),
            ["value_1", "value_2"],
        ),
        (
            sa.select(sa.literal(13).label("value_1")).with_statement_hint(", 79 AS value_2 WHERE false"),
            ["value_1", "value_2"],
        ),
        (
            sa.select(Number.number.op(",")(Number.number)).where(sa.false()),
            ["number", "anon_1"],
        ),
    ],
)
def test_empty_raw_projection_keeps_server_metadata(metadata_connection, client_mode, call, statement, expected_keys):
    connection = metadata_connection
    raw_connection = call(connection.get_raw_connection) if client_mode == "async" else connection.connection
    client = raw_connection.driver_connection
    if client_mode == "sync":
        client = client.client
    with patch.object(client, "query", wraps=client.query) as query:
        result = call(connection.execute, statement)

    assert list(result.keys()) == expected_keys
    assert result.all() == []
    assert query.call_count == 2


def test_empty_materialized_cte_keeps_compiled_metadata_shortcut(metadata_connection, client_mode, call):
    connection = metadata_connection
    raw_connection = call(connection.get_raw_connection) if client_mode == "async" else connection.connection
    client = raw_connection.driver_connection
    if client_mode == "sync":
        client = client.client
    source = cte(sa.select(Number.number).where(sa.false()), "empty_numbers", materialized=True)
    statement = sa.select(source.c.number).execution_options(settings={"enable_materialized_cte": 1, "enable_analyzer": 1})

    with patch.object(client, "query", wraps=client.query) as query:
        result = call(connection.execute, statement)

    assert list(result.keys()) == ["number"]
    assert result.all() == []
    assert query.call_count == 1


@pytest.mark.parametrize("label_style", [sa.LABEL_STYLE_NONE, sa.LABEL_STYLE_DISAMBIGUATE_ONLY])
def test_empty_self_join_preserves_cursor_description(metadata_connection, client_mode, call, label_style):
    source = sa.table("one", sa.column("dummy"), schema="system")
    left = source.alias("a")
    right = source.alias("b")
    statement = (
        sa.select(left.c.dummy, right.c.dummy).select_from(left.join(right, left.c.dummy == right.c.dummy)).set_label_style(label_style)
    )
    connection = metadata_connection
    raw_connection = call(connection.get_raw_connection) if client_mode == "async" else connection.connection
    client = raw_connection.driver_connection
    if client_mode == "sync":
        client = client.client
    populated = call(connection.execute, statement)
    populated_names = [column[0] for column in populated.cursor.description]
    populated_keys = list(populated.keys())
    assert len(populated.all()) == 1

    with patch.object(client, "query", wraps=client.query) as query:
        empty = call(connection.execute, statement.where(sa.false()))

    assert [column[0] for column in empty.cursor.description] == populated_names
    assert list(empty.keys()) == populated_keys
    assert empty.all() == []
    if label_style == sa.LABEL_STYLE_NONE:
        assert populated_names == ["dummy", "b.dummy"]
        assert populated_keys == ["dummy", "dummy"]
        assert query.call_count == 2
    else:
        assert populated_names == ["dummy", "dummy_1"]
        assert populated_keys == ["dummy", "dummy_1"]
        assert query.call_count == 1
