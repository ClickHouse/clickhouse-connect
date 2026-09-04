"""SQLAlchemy parameter compilation and multi-row INSERT tests."""

from datetime import timedelta
from unittest.mock import Mock, patch

import pytest
from sqlalchemy import Integer, String, TypeDecorator, bindparam, column, create_engine, func, insert, select, table, text, tuple_
from sqlalchemy.engine import Engine
from sqlalchemy.exc import CompileError

from clickhouse_connect import dbapi
from clickhouse_connect.cc_sqlalchemy.datatypes.sqltypes import Time
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


@pytest.mark.parametrize("server_side", [False, True], ids=["client-side", "server-side"])
@pytest.mark.parametrize(
    ("values", "client_sql", "server_sql", "expected_params"),
    [
        (
            [{"id": 13, "name": "user_1"}, {"id": 79, "name": "user_2"}],
            "INSERT INTO `events` (`id`, `name`) VALUES (%(id_m0)s, %(name_m0)s), (%(id_m1)s, %(name_m1)s)",
            "INSERT INTO `events` (`id`, `name`) VALUES ({id_m0:Int32}, {name_m0:String}), ({id_m1:Int32}, {name_m1:String})",
            {"id_m0": 13, "name_m0": "user_1", "id_m1": 79, "name_m1": "user_2"},
        ),
        (
            [(13, "user_1"), (79, "user_2")],
            "INSERT INTO `events` (`id`, `name`) VALUES (%(id_m0)s, %(name_m0)s), (%(id_m1)s, %(name_m1)s)",
            "INSERT INTO `events` (`id`, `name`) VALUES ({id_m0:Int32}, {name_m0:String}), ({id_m1:Int32}, {name_m1:String})",
            {"id_m0": 13, "name_m0": "user_1", "id_m1": 79, "name_m1": "user_2"},
        ),
        (
            [{"id": 13, "name": func.lower("USER_1")}, {"id": 79, "name": func.lower("USER_2")}],
            "INSERT INTO `events` (`id`, `name`) VALUES (%(id_m0)s, lower(%(lower_1)s)), (%(id_m1)s, lower(%(lower_2)s))",
            "INSERT INTO `events` (`id`, `name`) VALUES ({id_m0:Int32}, lower({lower_1:String})), ({id_m1:Int32}, lower({lower_2:String}))",
            {"id_m0": 13, "lower_1": "USER_1", "id_m1": 79, "lower_2": "USER_2"},
        ),
    ],
    ids=["dicts", "tuples", "per-row-expressions"],
)
def test_multivalues_insert_compiles(server_side, values, client_sql, server_sql, expected_params):
    compiled = _compile(insert(events).values(values), server_side=server_side)

    assert str(compiled) == (server_sql if server_side else client_sql)
    assert compiled.params == expected_params


class _StubColumnType:
    name = "String"


class _StubQueryResult:
    result_set = [["default"]]
    column_names = ["currentDatabase()"]
    column_types = [_StubColumnType()]
    summary = {}


class _StubInsertResult:
    written_rows = 2
    summary = {}


@pytest.fixture(name="routing_engine")
def routing_engine_fixture():
    client = Mock()
    client.server_tz = "UTC"
    client.query.return_value = _StubQueryResult()
    client.insert.return_value = _StubInsertResult()
    with patch("clickhouse_connect.dbapi.connection.create_client", return_value=client):
        engine: Engine = create_engine("clickhousedb://user_1:pwd@localhost:8123/default")
        try:
            yield engine, client
        finally:
            engine.dispose()


def test_multivalues_insert_dispatch(routing_engine):
    engine, client = routing_engine
    rows = [{"id": 13, "name": "user_1"}, {"id": 79, "name": "user_2"}]

    with engine.begin() as conn:
        client.query.reset_mock()
        client.insert.reset_mock()
        conn.execute(insert(events).values(rows))

        client.query.assert_called_once()
        client.insert.assert_not_called()

        client.query.reset_mock()
        conn.execute(insert(events), rows)

        client.query.assert_not_called()
        client.insert.assert_called_once_with("`events`", [[13, "user_1"], [79, "user_2"]], ["id", "name"], settings=None)


def test_double_percents_disabled_only_when_enabled():
    assert ClickHouseDialect(dbapi=dbapi, server_side_params=True).identifier_preparer._double_percents is False
    assert ClickHouseDialect(dbapi=dbapi).identifier_preparer._double_percents is True


def test_untyped_bind_raises():
    with pytest.raises(CompileError):
        _sql(select(events.c.id).where(text("id = :x")).params(x=13))


def test_non_word_bind_name_raises():
    with pytest.raises(CompileError):
        _sql(select(events.c.id).where(events.c.id == bindparam("a-b", value=13, type_=Integer())))


@pytest.mark.parametrize("name", ["id$x", "$x", "id$", "a$$b", "$1", "13_"])
def test_dollar_bind_name_renders_server_side(name):
    # These names each lex as one ASCII BareWord (issue #936).
    sql = _sql(select(events.c.id).where(events.c.id == bindparam(name, value=13, type_=Integer())))
    assert f"{{{name}:Int32}}" in sql


@pytest.mark.parametrize("name", ["$", "$$", "$$a", "13", "13$", "13a$x", "id\N{LATIN SMALL LETTER E WITH ACUTE}"])
def test_invalid_bareword_bind_name_raises(name):
    with pytest.raises(CompileError, match="ASCII BareWord"):
        _sql(select(events.c.id).where(events.c.id == bindparam(name, value=13, type_=Integer())))


def test_binary_reserved_bind_name_raises():
    with pytest.raises(CompileError, match="reserved for binary query parameters"):
        _sql(select(events.c.id).where(events.c.id == bindparam("$x$", value=13, type_=Integer())))


def test_binary_reserved_expanding_bind_name_raises():
    values = bindparam("$x$", value=[13, 79], type_=Integer(), expanding=True)
    with pytest.raises(CompileError, match="reserved for binary query parameters"):
        _sql(select(events.c.id).where(events.c.id.in_(values)))


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
