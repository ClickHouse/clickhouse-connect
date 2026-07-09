"""Compile-time tests for the opt-in server_side_params mode (issue #735)."""

import pytest
from sqlalchemy import Integer, String, bindparam, column, select, table, text, tuple_
from sqlalchemy.exc import CompileError

from clickhouse_connect import dbapi
from clickhouse_connect.cc_sqlalchemy.dialect import ClickHouseDialect

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
    from datetime import timedelta

    from clickhouse_connect.cc_sqlalchemy.datatypes.sqltypes import Time

    durations = table("durations", column("dur", Time()), column("id", Integer))
    with pytest.raises(CompileError):
        _sql(select(durations.c.id).where(durations.c.dur.in_([timedelta(seconds=5)])))


def test_literal_binds_keep_single_percent():
    sql_literal = str(
        select(events.c.id)
        .where(events.c.name == "pre%fix")
        .compile(dialect=ClickHouseDialect(dbapi=dbapi, server_side_params=True), compile_kwargs={"literal_binds": True})
    )
    assert "'pre%fix'" in sql_literal
    assert "%%" not in sql_literal
