"""Compile-time tests for the opt-in server_side_params mode (issue #735)."""

from datetime import timedelta

import pytest
from sqlalchemy import Integer, String, TypeDecorator, bindparam, column, literal, select, table, text, tuple_
from sqlalchemy.exc import CompileError

from clickhouse_connect import dbapi
from clickhouse_connect.cc_sqlalchemy.datatypes.sqltypes import String as ChString
from clickhouse_connect.cc_sqlalchemy.datatypes.sqltypes import Time
from clickhouse_connect.cc_sqlalchemy.dialect import ClickHouseDialect
from clickhouse_connect.driver.binding import format_str

events = table("events", column("id", Integer), column("name", String))


def _compile(element, server_side=True):
    dialect = ClickHouseDialect(dbapi=dbapi, server_side_params=server_side)
    return element.compile(dialect=dialect)


def _sql(element, server_side=True):
    return str(_compile(element, server_side))


def test_scalar_binds_render_server_side():
    sql = _sql(select(events.c.id).where(events.c.name == "user_1").where(events.c.id > 13))
    assert "{name_1:String}" in sql
    assert "{id_1:Int32}" in sql
    assert "%(" not in sql


def test_between_renders_two_scalar_binds():
    sql = _sql(select(events.c.id).where(events.c.id.between(13, 79)))
    assert "BETWEEN {id_1:Int32} AND {id_2:Int32}" in sql


def test_in_renders_single_array_placeholder():
    sql = _sql(select(events.c.id).where(events.c.id.in_([13, 79, 5])))
    assert "IN {id_1:Array(Int32)}" in sql
    assert "POSTCOMPILE" not in sql
    assert "IN ({id_1" not in sql


def test_not_in_renders_array_placeholder():
    sql = _sql(select(events.c.id).where(events.c.id.notin_([13, 79])))
    assert "{id_1:Array(Int32)}" in sql
    assert "POSTCOMPILE" not in sql


def test_tuple_in_renders_array_of_tuple():
    sql = _sql(select(events.c.id).where(tuple_(events.c.id, events.c.name).in_([(13, "u1"), (79, "u2")])))
    assert "{param_1:Array(Tuple(Int32, String))}" in sql
    assert "POSTCOMPILE" not in sql


def test_mixed_scalar_and_in_has_no_pyformat():
    sql = _sql(select(events.c.id).where(events.c.name == "u2").where(events.c.id.in_([13, 79])))
    assert "{name_1:String}" in sql
    assert "{id_1:Array(Int32)}" in sql
    assert "%(" not in sql


def test_modulo_renders_single_percent():
    sql = _sql(select(events.c.id).where(events.c.id % 5 == 0))
    assert "%%" not in sql
    assert " % " in sql


def test_limit_offset_render_server_side():
    sql = _sql(select(events.c.id).limit(10).offset(5))
    assert "{param_1:Int32}" in sql
    assert "{param_2:Int32}" in sql


def test_param_dict_keys_match_bind_names():
    compiled = _compile(select(events.c.id).where(events.c.name == "user_1").where(events.c.id > 13))
    assert set(compiled.params) == {"name_1", "id_1"}


def test_flag_off_keeps_pyformat():
    sql = _sql(select(events.c.id).where(events.c.id == 13), server_side=False)
    assert "%(id_1)s" in sql
    assert "{id_1:" not in sql


def test_double_percents_disabled_only_when_enabled():
    assert ClickHouseDialect(dbapi=dbapi, server_side_params=True).identifier_preparer._double_percents is False
    assert ClickHouseDialect(dbapi=dbapi).identifier_preparer._double_percents is True


def test_untyped_bind_raises():
    with pytest.raises(CompileError):
        _sql(select(events.c.id).where(text("id = :x")).params(x=13))


def test_non_word_bind_name_raises():
    with pytest.raises(CompileError):
        _sql(select(events.c.id).where(events.c.id == bindparam("a-b", value=13, type_=Integer())))


def test_in_list_with_bind_processor_raises():
    class ProcessedString(TypeDecorator):
        impl = String
        cache_ok = True

        def process_bind_param(self, value, dialect):
            return value.upper()

    processed = table("processed", column("tag", ProcessedString()), column("id", Integer))
    with pytest.raises(CompileError, match="bind processor"):
        _sql(select(processed.c.id).where(processed.c.tag.in_(["user_1"])))


def test_in_list_of_time_renders_array_placeholder():
    # Time no longer inherits Interval's epoch bind processor, so IN lists bind natively.
    durations = table("durations", column("dur", Time()), column("id", Integer))
    sql = _sql(select(durations.c.id).where(durations.c.dur.in_([timedelta(seconds=5)])))
    assert "{dur_1:Array(Time)}" in sql


def test_literal_binds_keep_single_percent():
    sql_literal = str(
        select(events.c.id)
        .where(events.c.name == "pre%fix")
        .compile(dialect=ClickHouseDialect(dbapi=dbapi, server_side_params=True), compile_kwargs={"literal_binds": True})
    )
    assert "'pre%fix'" in sql_literal
    assert "%%" not in sql_literal


@pytest.mark.parametrize("type_", [None, String()])
def test_literal_binds_escape_backslashes_and_preserve_percents(type_):
    payload = "\\', 79% AS injected --"
    value = literal(payload) if type_ is None else literal(payload, type_=type_)
    sql_literal = str(
        select(value).compile(
            dialect=ClickHouseDialect(dbapi=dbapi),
            compile_kwargs={"literal_binds": True},
        )
    )

    assert "'\\\\'', 79%% AS injected --'" in sql_literal
    assert sql_literal.count("%") == 2


def test_literal_binds_preserve_type_decorator_input():
    seen = []

    class TrackingString(TypeDecorator):
        impl = String
        cache_ok = True

        def process_literal_param(self, value, dialect):
            seen.append(value)
            return value

    payload = "path\\'segment"
    sql_literal = str(
        select(literal(payload, type_=TrackingString())).compile(
            dialect=ClickHouseDialect(dbapi=dbapi),
            compile_kwargs={"literal_binds": True},
        )
    )

    assert seen == [payload]
    assert sql_literal == "SELECT 'path\\\\''segment' AS `anon_1`"


def test_literal_binds_do_not_double_escape_clickhouse_types():
    payload = "path\\'segment"
    sql_literal = str(
        select(literal(payload, type_=ChString())).compile(
            dialect=ClickHouseDialect(dbapi=dbapi),
            compile_kwargs={"literal_binds": True},
        )
    )

    assert sql_literal == f"SELECT {format_str(payload)} AS `anon_1`"
