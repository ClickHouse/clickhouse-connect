import time
from collections import namedtuple
from contextlib import ExitStack
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import Mock, patch

import pytest
import sqlalchemy as sa

from clickhouse_connect import common, dbapi
from clickhouse_connect.cc_sqlalchemy.datatypes.base import sqla_type_from_name
from clickhouse_connect.cc_sqlalchemy.datatypes.sqltypes import Array, DateTime, DateTime64, Nullable, Tuple, UInt32
from clickhouse_connect.cc_sqlalchemy.ddl.tableengine import MergeTree
from clickhouse_connect.cc_sqlalchemy.dialect import ClickHouseDialect
from clickhouse_connect.driver.binding import finalize_query, str_query_value

STAMP = datetime(2024, 6, 13, 7, 8, 9, 123456, tzinfo=timezone.utc)
LITERAL = "'2024-06-13 07:08:09.123456'"
SECONDS = "'2024-06-13 07:08:09'"


class WrappedDateTime64(sa.TypeDecorator):
    impl = DateTime64(6)
    cache_ok = True


class WrappedDateTime64Array(sa.TypeDecorator):
    impl = sa.ARRAY(WrappedDateTime64(), dimensions=2)
    cache_ok = True


@pytest.mark.parametrize("server_side", [False, True])
@pytest.mark.parametrize(
    ("type_", "value", "expected"),
    [
        (DateTime64(6), STAMP, LITERAL),
        (Array(DateTime64(6)), [STAMP], f"[{LITERAL}]"),
        (Tuple(DateTime64(6), DateTime()), (STAMP, STAMP), f"({LITERAL}, {SECONDS})"),
        (Array(Tuple(UInt32, DateTime64(6))), [(13, STAMP)], f"[(13, {LITERAL})]"),
        (Nullable(DateTime64(6)), STAMP, LITERAL),
        (Array(Nullable(DateTime64(6))), [None, STAMP], f"[NULL, {LITERAL}]"),
        (sqla_type_from_name("Tuple(id UInt32, stamp DateTime64(6))"), (13, STAMP), f"(13, {LITERAL})"),
        (sqla_type_from_name("datetime64(6)"), STAMP, LITERAL),
        (sqla_type_from_name("DaTeTiMe64(6)"), STAMP, LITERAL),
        (DateTime(), STAMP, SECONDS),
        (Nullable(DateTime64(6)), None, "NULL"),
        (DateTime64(6), "2024-06-13 07:08:09.123456", LITERAL),
        (DateTime64(6), 1718262489, "1718262489"),
    ],
    ids=[
        "scalar",
        "array",
        "mixed-tuple",
        "array-tuple",
        "nullable",
        "array-nullable",
        "named-tuple",
        "lowercase",
        "mixed-case",
        "datetime",
        "null",
        "string",
        "integer",
    ],
)
def test_datetime64_literals(type_, value, expected, server_side):
    dialect = ClickHouseDialect(dbapi=dbapi, server_side_params=server_side)
    compiled = sa.select(sa.literal(value, type_=type_)).compile(dialect=dialect, compile_kwargs={"literal_binds": True})

    assert str(compiled) == f"SELECT {expected} AS `anon_1`"


@pytest.mark.parametrize("type_", [DateTime64(0), DateTime64(3), DateTime64(6), DateTime64(9)])
def test_datetime64_literal_preserves_input_precision_for_server_coercion(type_):
    processor = type_.literal_processor(ClickHouseDialect(dbapi=dbapi))

    assert processor(STAMP) == LITERAL
    assert processor(STAMP.replace(microsecond=0)) == "'2024-06-13 07:08:09.000000'"


@pytest.mark.parametrize("mode", ["legacy", "wall"])
@pytest.mark.parametrize("aware", [False, True])
@pytest.mark.parametrize("server_tz", [timezone.utc, timezone(timedelta(hours=5, minutes=30))])
def test_datetime64_keeps_sql_text_timezone_behavior(monkeypatch, mode, aware, server_tz):
    monkeypatch.setenv("TZ", "America/Los_Angeles")
    previous_mode = common.get_setting("naive_datetime_binding")
    common.set_setting("naive_datetime_binding", mode)
    if hasattr(time, "tzset"):
        time.tzset()
    try:
        value = STAMP if aware else STAMP.replace(tzinfo=None)
        dialect = ClickHouseDialect(dbapi=dbapi)
        statement = sa.select(sa.bindparam("stamp", value, type_=DateTime64(6)))
        compiled = statement.compile(dialect=dialect)
        context = SimpleNamespace(compiled=compiled, execution_options={}, invoked_statement=statement)
        parameters = {"stamp": value}
        cursor = Mock()

        dialect.do_execute(cursor, str(compiled), parameters, context)

        bound_query, bound_parameters = cursor.execute.call_args.args
        expected = str_query_value(value, server_tz)[:-1] + ".123456'"
        assert finalize_query(bound_query, bound_parameters, server_tz) == f"SELECT {expected} AS `anon_1`"
        assert parameters == {"stamp": value}
        literal_expected = str_query_value(value, timezone.utc)[:-1] + ".123456'"
        assert DateTime64(6).literal_processor(dialect)(value) == literal_expected
    finally:
        common.set_setting("naive_datetime_binding", previous_mode)
        monkeypatch.undo()
        if hasattr(time, "tzset"):
            time.tzset()


def test_datetime64_wrong_tuple_shape_reaches_existing_formatter():
    type_ = Tuple(DateTime64(6), UInt32)
    value = (STAMP,)

    assert type_.literal_processor(ClickHouseDialect(dbapi=dbapi))(value) == str_query_value(value)


def test_datetime64_array_accepts_tuple_subclasses():
    values = namedtuple("Values", ["first", "second"])(STAMP, STAMP)

    rendered = Array(DateTime64(6)).literal_processor(ClickHouseDialect(dbapi=dbapi))(values)

    assert rendered.count(LITERAL) == 2


@pytest.mark.parametrize(
    ("first_type", "second_type", "value", "expected"),
    [
        (DateTime(), DateTime64(6), STAMP, SECONDS),
        (DateTime64(6), DateTime(), STAMP, SECONDS),
        (DateTime64(3), DateTime64(6), STAMP, LITERAL),
        (Array(DateTime()), Array(DateTime64(6)), [STAMP], f"[{SECONDS}]"),
        (Array(DateTime64(3)), Array(DateTime64(6)), [STAMP], f"[{LITERAL}]"),
        (Tuple(DateTime64(6), DateTime()), Tuple(DateTime(), DateTime64(6)), (STAMP, STAMP), f"({SECONDS}, {SECONDS})"),
        (sa.sql.sqltypes.TupleType(DateTime64(6), UInt32()), Tuple(DateTime64(6), UInt32), (STAMP, 13), f"({LITERAL}, 13)"),
        (sa.sql.sqltypes.TupleType(DateTime64(6), DateTime()), Tuple(DateTime(), DateTime64(6)), (STAMP, STAMP), f"({SECONDS}, {SECONDS})"),
        (sa.sql.sqltypes.TupleType(DateTime(), UInt32()), Tuple(DateTime64(6), UInt32), (STAMP, 13), f"({SECONDS}, 13)"),
    ],
)
def test_shared_bind_preserves_compatible_formatting(first_type, second_type, value, expected):
    table = sa.Table("events", sa.MetaData(), sa.Column("first", first_type), sa.Column("second", second_type))
    statement = table.insert().values(first=sa.bindparam("stamp"), second=sa.bindparam("stamp"))
    dialect = ClickHouseDialect(dbapi=dbapi)
    compiled = statement.compile(dialect=dialect)
    context = SimpleNamespace(compiled=compiled, execution_options={}, invoked_statement=statement)
    cursor = Mock()

    dialect.do_execute(cursor, str(compiled), {"stamp": value}, context)

    query, parameters = cursor.execute.call_args.args
    assert finalize_query(query, parameters, timezone.utc) == f"INSERT INTO `events` (`first`, `second`) VALUES ({expected}, {expected})"


@pytest.mark.parametrize("parameters", [{}, {"stamp": STAMP}])
def test_datetime64_adaptation_leaves_ddl_parameters_unchanged(parameters):
    table = sa.Table("events", sa.MetaData(), sa.Column("stamp", DateTime64(6)), MergeTree(order_by="stamp"))
    statement = sa.schema.CreateTable(table)
    dialect = ClickHouseDialect(dbapi=dbapi)
    compiled = statement.compile(dialect=dialect)
    context = SimpleNamespace(compiled=compiled, execution_options={}, invoked_statement=statement)
    cursor = Mock()

    dialect.do_execute(cursor, str(compiled), parameters, context)

    assert cursor.execute.call_args.args == (str(compiled), parameters)
    assert cursor.execute.call_args.args[1] is parameters


@pytest.mark.parametrize(
    ("type_", "value", "expected"),
    [
        (sa.ARRAY(DateTime64(6)), [STAMP], f"[{LITERAL}]"),
        (sa.ARRAY(DateTime64(6)), [[STAMP]], f"[[{LITERAL}]]"),
        (sa.ARRAY(DateTime64(6), dimensions=2), [[STAMP], None], f"[[{LITERAL}], NULL]"),
        (sa.ARRAY(DateTime64(6), dimensions=3), [[[STAMP]]], f"[[[{LITERAL}]]]"),
        (sa.ARRAY(DateTime64(6), dimensions=1, as_tuple=True), [STAMP], f"[{LITERAL}]"),
        (sa.ARRAY(Nullable(DateTime64(6))), [STAMP, None], f"[{LITERAL}, NULL]"),
        (sa.ARRAY(Tuple(DateTime64(6), DateTime()), dimensions=1), [(STAMP, STAMP)], f"[({LITERAL}, {SECONDS})]"),
        (sa.ARRAY(Tuple(DateTime64(6), DateTime())), [(STAMP, STAMP)], f"[({LITERAL}, {SECONDS})]"),
        (sa.ARRAY(Tuple(DateTime64(6), DateTime())), [[(STAMP, STAMP)]], f"[[({LITERAL}, {SECONDS})]]"),
        (sa.ARRAY(Tuple(DateTime64(6), DateTime())), [(STAMP,)], f"[({SECONDS})]"),
        (sa.ARRAY(Tuple(DateTime64(6), DateTime())), [[STAMP, STAMP]], f"[[{SECONDS}, {SECONDS}]]"),
        (sa.ARRAY(Tuple(DateTime64(6), DateTime())), [None, (STAMP, STAMP)], f"[NULL, ({LITERAL}, {SECONDS})]"),
        (
            sa.ARRAY(Tuple(DateTime(), Array(DateTime64(6)))),
            [((STAMP, [STAMP]), (STAMP, [STAMP]))],
            f"[(({SECONDS}, [{LITERAL}]), ({SECONDS}, [{LITERAL}]))]",
        ),
        (
            sa.ARRAY(Tuple(DateTime64(6), DateTime())),
            [((STAMP, STAMP), (STAMP, STAMP))],
            f"[(({LITERAL}, {SECONDS}), ({LITERAL}, {SECONDS}))]",
        ),
        (sa.ARRAY(Tuple(Array(UInt32), DateTime64(6))), [([13], STAMP)], f"[([13], {LITERAL})]"),
        (sa.ARRAY(Tuple(Tuple(UInt32, UInt32), DateTime64(6))), [((13, 79), STAMP)], f"[((13, 79), {LITERAL})]"),
        (sa.ARRAY(Tuple(Tuple(DateTime(), UInt32), DateTime64(6))), [((STAMP, 13), STAMP)], f"[(({SECONDS}, 13), {LITERAL})]"),
        (sa.ARRAY(DateTime64(6)), ((STAMP, STAMP),), f"(({LITERAL}, {LITERAL}))"),
        (WrappedDateTime64Array(), [[STAMP]], f"[[{LITERAL}]]"),
        (sa.ARRAY(sa.DateTime().with_variant(DateTime64(6), "clickhousedb")), [STAMP], f"[{LITERAL}]"),
        (sa.ARRAY(DateTime64(6)), [], "[]"),
        (sa.ARRAY(DateTime64(6)), None, "NULL"),
        (sa.ARRAY(DateTime64(6)), ["2024-06-13 07:08:09.123456", 13, None], f"[{LITERAL}, 13, NULL]"),
        (sa.ARRAY(DateTime()), [STAMP], f"[{SECONDS}]"),
    ],
)
def test_generic_array_datetime64_binds(type_, value, expected):
    dialect = ClickHouseDialect(dbapi=dbapi)
    statement = sa.select(sa.bindparam("stamps", value, type_))
    compiled = statement.compile(dialect=dialect)
    context = SimpleNamespace(compiled=compiled, execution_options={}, invoked_statement=statement)
    cursor = Mock()
    parameters = {"stamps": value}

    dialect.do_execute(cursor, str(compiled), parameters, context)

    query, bound_parameters = cursor.execute.call_args.args
    assert finalize_query(query, bound_parameters, timezone.utc) == f"SELECT {expected} AS `anon_1`"
    assert parameters == {"stamps": value}


@pytest.mark.parametrize("sibling_type, sibling", [(Array(UInt32), [13, 79]), (Tuple(UInt32, UInt32), (13, 79))])
def test_generic_array_preserves_non_temporal_container_identity(sibling_type, sibling):
    dialect = ClickHouseDialect(dbapi=dbapi)
    statement = sa.select(sa.bindparam("stamps", type_=sa.ARRAY(Tuple(sibling_type, DateTime64(6)))))
    compiled = statement.compile(dialect=dialect)
    context = SimpleNamespace(compiled=compiled, execution_options={}, invoked_statement=statement)
    cursor = Mock()

    dialect.do_execute(cursor, str(compiled), {"stamps": [(sibling, STAMP)]}, context)

    assert cursor.execute.call_args.args[1]["stamps"][0][0] is sibling


@pytest.mark.parametrize("type_", [sa.Integer(), DateTime64(6), WrappedDateTime64()])
@pytest.mark.parametrize("tuple_bind", [False, True])
def test_expanding_bind_resolves_types_once(type_, tuple_bind):
    dialect = ClickHouseDialect(dbapi=dbapi)
    values = [79 if isinstance(type_, sa.Integer) else STAMP] * 13
    column = sa.column("stamp", type_)
    if tuple_bind:
        values = [(value, 13) for value in values]
        column = sa.tuple_(column, sa.column("id", UInt32()))
    statement = sa.select(column.in_(sa.bindparam("ts%val", values, type_=column.type, expanding=True)))
    compiled = statement.compile(dialect=dialect)
    expanded = compiled._process_parameters_for_postcompile(compiled.construct_params(escape_names=False))
    context = SimpleNamespace(
        compiled=compiled,
        execution_options={},
        invoked_statement=statement,
        _expanded_parameters=expanded.parameter_expansion,
    )
    bind_type = next(iter(compiled.bind_names)).type
    types = bind_type.types if tuple_bind else (bind_type,)
    cursor = Mock()

    with ExitStack() as stack:
        resolvers = [stack.enter_context(patch.object(type_, "dialect_impl", wraps=type_.dialect_impl)) for type_ in types]
        dialect.do_execute(cursor, expanded.statement, expanded.additional_parameters, context)

    assert [resolver.call_count for resolver in resolvers] == [1] * len(types)
    query, parameters = cursor.execute.call_args.args
    rendered = finalize_query(query, parameters, timezone.utc)
    assert rendered.count("79" if isinstance(type_, sa.Integer) else LITERAL) == 13


@pytest.mark.parametrize("type_", [sa.Integer(), DateTime64(6), WrappedDateTime64()])
def test_fallback_executemany_resolves_types_once(type_):
    dialect = ClickHouseDialect(dbapi=dbapi)
    table = sa.table("events", sa.column("stamp", type_))
    statement = table.insert().values(stamp=sa.bindparam("stamp", type_=type_))
    compiled = statement.compile(dialect=dialect)
    context = SimpleNamespace(compiled=compiled, statement=str(compiled), execution_options={}, invoked_statement=statement)
    rows = [{"stamp": 79 if isinstance(type_, sa.Integer) else STAMP}] * 13
    bind_type = next(iter(compiled.bind_names)).type
    cursor = Mock()

    with patch.object(bind_type, "dialect_impl", wraps=bind_type.dialect_impl) as resolver:
        dialect.do_executemany(cursor, str(compiled), rows, context)

    assert resolver.call_count == 1
    query, parameters = cursor.executemany.call_args.args
    expected = "79" if isinstance(type_, sa.Integer) else LITERAL
    assert all(expected in finalize_query(query, row, timezone.utc) for row in parameters)
    if isinstance(type_, sa.Integer):
        assert parameters is rows


@pytest.mark.parametrize("expanding", [False, True])
@pytest.mark.parametrize(
    ("types", "value", "expected"),
    [
        ((UInt32(), DateTime64(6)), (13, STAMP), f"(13, {LITERAL})"),
        ((DateTime64(6), UInt32()), (STAMP, 13), f"({LITERAL}, 13)"),
        ((DateTime(), DateTime64(6)), (STAMP, STAMP), f"({SECONDS}, {LITERAL})"),
        ((UInt32(), Tuple(DateTime64(6), DateTime())), (13, (STAMP, STAMP)), f"(13, ({LITERAL}, {SECONDS}))"),
        ((UInt32(), UInt32()), (13, 79), "(13, 79)"),
    ],
)
def test_tupletype_datetime64_binds_at_dialect_boundary(types, value, expected, expanding):
    """Exercise tuple signatures with a synthetic post-processor execution context."""
    dialect = ClickHouseDialect(dbapi=dbapi)
    columns = sa.tuple_(*(sa.column(f"column_{index}", type_) for index, type_ in enumerate(types)))
    bind = sa.bindparam("value", type_=columns.type, expanding=expanding)
    statement = sa.select(columns.in_(bind) if expanding else columns == bind)
    compiled = statement.compile(dialect=dialect)
    context = SimpleNamespace(compiled=compiled, execution_options={}, invoked_statement=statement)
    query = str(compiled)
    parameters = {"value": value}
    if expanding:
        expanded = compiled._process_parameters_for_postcompile({"value": [value]})
        query = expanded.statement
        parameters = expanded.additional_parameters
        context._expanded_parameters = expanded.parameter_expansion
    cursor = Mock()

    dialect.do_execute(cursor, query, parameters, context)

    query, bound_parameters = cursor.execute.call_args.args
    assert expected in finalize_query(query, bound_parameters, timezone.utc)
    if all(isinstance(type_, UInt32) for type_ in types):
        assert bound_parameters is parameters
