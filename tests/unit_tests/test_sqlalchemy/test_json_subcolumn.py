import pytest
from sqlalchemy import Column, Integer, column, select, table
from sqlalchemy.orm import declarative_base
from sqlalchemy.sql import visitors
from sqlalchemy.sql.elements import Cast
from sqlalchemy.sql.sqltypes import NullType

from clickhouse_connect import dbapi
from clickhouse_connect.cc_sqlalchemy import json_subcolumn
from clickhouse_connect.cc_sqlalchemy.datatypes.sqltypes import JSON, UInt32
from clickhouse_connect.cc_sqlalchemy.dialect import ClickHouseDialect
from clickhouse_connect.driver.binding import finalize_query


def compile_sql(expression):
    return str(expression.compile(dialect=ClickHouseDialect()))


def test_json_index_compiles_as_dotted_subcolumn():
    events = table("events", column("payload", JSON()))
    expression = events.c.payload["severity"]

    sql = compile_sql(select(expression))

    assert sql == "SELECT `events`.`payload`.`severity` AS `anon_1` \nFROM `events`"
    assert "getSubcolumn" not in sql
    assert isinstance(expression.type, NullType)


def test_json_subcolumn_compiles_on_default_dialect():
    events = table("events", column("payload", JSON()))

    sql = str(select(events.c.payload["context"].subcolumn("id", type_=UInt32)))

    assert 'payload."context"."id"' in sql
    assert "getSubcolumn" not in sql


def test_json_subcolumn_method_and_nested_paths():
    events = table("events", column("payload", JSON()))

    expression = events.c.payload.subcolumn("context")["request"].subcolumn("id")

    assert compile_sql(expression) == "`events`.`payload`.`context`.`request`.`id`"
    assert "getSubcolumn" not in compile_sql(expression)
    assert isinstance(expression.type, NullType)


def test_json_subcolumn_helper_matches_column_method():
    events = table("events", column("payload", JSON()))

    helper_expression = json_subcolumn(json_subcolumn(events.c.payload, "context"), "id", type_=UInt32)
    method_expression = events.c.payload.subcolumn("context").subcolumn("id", type_=UInt32)

    assert compile_sql(helper_expression) == compile_sql(method_expression)
    assert type(helper_expression.type) is type(method_expression.type)


@pytest.mark.parametrize(
    ("segment", "quoted"),
    [
        ("space key", "`space key`"),
        ("quoted`key", "`quoted\\`key`"),
        ("`wrapped`", "`\\`wrapped\\``"),
        (":kind", "`:kind`"),
        ("^kind", "`^kind`"),
        ("@kind", "`@kind`"),
        ("a.b", "`a.b`"),
        ("a%2Eb", "`a%2Eb`"),
    ],
)
def test_json_subcolumn_quotes_each_segment_without_dot_encoding(segment, quoted):
    events = table("events", column("payload", JSON()))

    assert compile_sql(events.c.payload[segment]) == f"`events`.`payload`.{quoted}"


@pytest.mark.parametrize("segment", ["a%2Eb", "%(name)s"])
def test_json_subcolumn_percent_segment_survives_pyformat_interpolation(segment):
    events = table("events", column("id", Integer), column("payload", JSON()))
    statement = select(events.c.payload[segment]).where(events.c.id == 13)
    compiled = statement.compile(dialect=ClickHouseDialect(dbapi=dbapi))

    escaped_segment = segment.replace("%", "%%")
    assert f"`{escaped_segment}`" in compiled.string

    final_sql = finalize_query(compiled.string, compiled.params)
    assert f"`{segment}`" in final_sql
    assert " = 13" in final_sql


def test_json_subcolumn_percent_segment_is_not_doubled_for_server_side_params():
    events = table("events", column("id", Integer), column("payload", JSON()))
    statement = select(events.c.payload["a%2Eb"]).where(events.c.id == 13)
    compiled = statement.compile(dialect=ClickHouseDialect(dbapi=dbapi, server_side_params=True))

    assert "`a%2Eb`" in compiled.string
    assert "`a%%2Eb`" not in compiled.string
    assert "{id_1:Int32}" in compiled.string


def test_json_subcolumn_uses_alias_and_discovers_from_clause():
    events = table("events", column("payload", JSON()))
    event_alias = events.alias("evt")
    statement = select(event_alias.c.payload["severity"])

    assert compile_sql(statement) == "SELECT `evt`.`payload`.`severity` AS `anon_1` \nFROM `events` AS `evt`"
    assert statement.get_final_froms() == [event_alias]


@pytest.mark.parametrize(
    ("type_arg", "expected_type", "compiled_type"),
    [(UInt32, UInt32, "UInt32"), (Integer, Integer, "INTEGER")],
)
def test_json_subcolumn_type_uses_cast_and_sets_result_type(type_arg, expected_type, compiled_type):
    events = table("events", column("payload", JSON()))

    expression = events.c.payload.subcolumn("count", type_=type_arg)
    sql = compile_sql(expression)

    assert isinstance(expression, Cast)
    assert isinstance(expression.type, expected_type)
    assert sql == f"CAST(`events`.`payload`.`count` AS {compiled_type})"
    assert "getSubcolumn" not in sql


def test_json_untyped_comparison_uses_value_type_for_bind():
    events = table("events", column("payload", JSON()))

    predicate = events.c.payload["count"] == 13

    assert isinstance(predicate.right.type, Integer)


@pytest.mark.parametrize("accessor", [lambda payload: payload[13], lambda payload: payload.subcolumn(None)])
def test_json_subcolumn_rejects_non_string_segment(accessor):
    events = table("events", column("payload", JSON()))

    with pytest.raises(TypeError, match="JSON subcolumn path segment must be a string"):
        accessor(events.c.payload)


def test_json_subcolumn_rejects_empty_segment():
    events = table("events", column("payload", JSON()))

    with pytest.raises(ValueError, match="JSON subcolumn path segment must not be empty"):
        events.c.payload[""]


@pytest.mark.parametrize(
    "parent",
    ["payload", table("events", column("payload", JSON())), column("left") + column("right")],
)
def test_json_subcolumn_helper_rejects_non_column_parent(parent):
    with pytest.raises(TypeError, match="JSON subcolumn parent must be a SQLAlchemy column or JSON subcolumn"):
        json_subcolumn(parent, "severity")


def test_json_subcolumn_preserves_orm_propagation_attributes():
    base = declarative_base()

    class Event(base):
        __tablename__ = "event"

        id = Column(Integer, primary_key=True)
        payload = Column(JSON)

    expression = Event.payload["severity"]

    assert expression._propagate_attrs == Event.payload._propagate_attrs


def test_json_subcolumn_clone_and_cache_traversal_preserve_path():
    events = table("events", column("payload", JSON()))
    statement = select(events.c.payload["context"]["request"])

    cloned = visitors.cloned_traverse(statement, {}, {})
    assert compile_sql(cloned) == compile_sql(statement)
    assert cloned.get_final_froms() == statement.get_final_froms()

    nested_key = statement._generate_cache_key()
    dotted_key = select(events.c.payload["context.request"])._generate_cache_key()
    other_key = select(events.c.payload["context"]["response"])._generate_cache_key()
    assert nested_key is not None and dotted_key is not None and other_key is not None
    assert nested_key != dotted_key
    assert nested_key != other_key
