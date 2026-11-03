from datetime import datetime
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
import sqlalchemy as sa

from clickhouse_connect import common, dbapi
from clickhouse_connect.cc_sqlalchemy.datatypes.base import sqla_type_from_name
from clickhouse_connect.cc_sqlalchemy.dialect import ClickHouseDialect
from clickhouse_connect.driver.binding import bind_query, finalize_query
from clickhouse_connect.driver.exceptions import ProgrammingError
from tests.unit_tests.test_driver.test_named_tuple_binding import NAMED, SHAPES, VALUE


@pytest.mark.parametrize(
    ("type_name", "value", "expected"), [case for case in SHAPES if case[0].startswith(("Tuple", "Array", "Nullable", "Map"))]
)
@pytest.mark.parametrize("server_side", [False, True])
def test_typed_named_tuple_parameters(type_name, value, expected, server_side):
    previous = common.get_setting("dict_parameter_format")
    common.set_setting("dict_parameter_format", "map")
    try:
        dialect = ClickHouseDialect(dbapi=dbapi, server_side_params=server_side)
        type_ = sqla_type_from_name(type_name)
        statement = sa.select(sa.bindparam("p", value, type_))
        compiled = statement.compile(dialect=dialect)
        context = SimpleNamespace(compiled=compiled, execution_options={}, invoked_statement=statement)
        cursor = Mock()
        parameters = {"p": value}
        dialect.do_execute(cursor, str(compiled), parameters, context)
        sql, bound = cursor.execute.call_args.args
        if server_side:
            assert bind_query(sql, bound)[1] == {"param_p": expected}
        else:
            expected_literal = "NULL" if value is None else expected
            assert finalize_query(sql, bound) == f"SELECT {expected_literal} AS `anon_1`"
        assert parameters == {"p": value}
        expected_literal = "NULL" if value is None else expected
        assert str(statement.compile(dialect=dialect, compile_kwargs={"literal_binds": True})) == f"SELECT {expected_literal} AS `anon_1`"
    finally:
        common.set_setting("dict_parameter_format", previous)


@pytest.mark.parametrize("dimensions", [None, 1, 2])
def test_generic_array_named_tuples(dimensions):
    type_ = sa.ARRAY(sqla_type_from_name(NAMED), dimensions=dimensions)
    value = [VALUE] if dimensions == 1 else [[VALUE]]
    dialect = ClickHouseDialect(dbapi=dbapi)
    statement = sa.select(sa.bindparam("p", value, type_))
    compiled = statement.compile(dialect=dialect)
    cursor = Mock()
    dialect.do_execute(
        cursor, str(compiled), {"p": value}, SimpleNamespace(compiled=compiled, execution_options={}, invoked_statement=statement)
    )
    sql, bound = cursor.execute.call_args.args
    expected = "[(13, 79)]" if dimensions == 1 else "[[(13, 79)]]"
    assert finalize_query(sql, bound) == f"SELECT {expected} AS `anon_1`"


@pytest.mark.parametrize(("named", "value_as_dict"), [(False, False), (True, False), (True, True)])
@pytest.mark.parametrize(
    ("sibling_type", "sibling", "literal"),
    [
        ("Array(UInt8)", [13], "[13]"),
        ("Map(String, UInt8)", {"user_1": 13}, "{'user_1':13}"),
        ("Nullable(Tuple(key UInt8))", None, "NULL"),
        ("Map(String, Tuple(key UInt8))", {"user_1": {"key": 13}}, "{'user_1':(13)}"),
    ],
)
def test_generic_array_named_tuple_keeps_container_shape(named, value_as_dict, sibling_type, sibling, literal):
    stamp = datetime(2024, 6, 13, 7, 8, 9, 123456)
    tuple_type = f"Tuple(stamp DateTime64(6), tags {sibling_type})" if named else f"Tuple(DateTime64(6), {sibling_type})"
    type_ = sa.ARRAY(sqla_type_from_name(tuple_type))
    value = [{"tags": sibling, "stamp": stamp}] if value_as_dict else [(stamp, sibling)]
    dialect = ClickHouseDialect(dbapi=dbapi)
    statement = sa.select(sa.bindparam("p", value, type_))
    compiled = statement.compile(dialect=dialect)
    cursor = Mock()
    previous = common.get_setting("dict_parameter_format")
    common.set_setting("dict_parameter_format", "map")
    try:
        dialect.do_execute(
            cursor, str(compiled), {"p": value}, SimpleNamespace(compiled=compiled, execution_options={}, invoked_statement=statement)
        )
        sql, bound = cursor.execute.call_args.args
        assert finalize_query(sql, bound) == f"SELECT [('2024-06-13 07:08:09.123456', {literal})] AS `anon_1`"
    finally:
        common.set_setting("dict_parameter_format", previous)


def test_named_tuple_datetime64_and_decorator():
    class WrappedTuple(sa.TypeDecorator):
        impl = sqla_type_from_name("Tuple(stamp DateTime64(6), protocol UInt8)")
        cache_ok = True

    value = {"protocol": 13, "stamp": datetime(2024, 6, 13, 7, 8, 9, 123456)}
    dialect = ClickHouseDialect(dbapi=dbapi)
    statement = sa.select(sa.bindparam("p", value, WrappedTuple()))
    compiled = statement.compile(dialect=dialect)
    cursor = Mock()
    dialect.do_execute(
        cursor, str(compiled), {"p": value}, SimpleNamespace(compiled=compiled, execution_options={}, invoked_statement=statement)
    )
    sql, bound = cursor.execute.call_args.args
    assert finalize_query(sql, bound) == "SELECT ('2024-06-13 07:08:09.123456', 13) AS `anon_1`"


@pytest.mark.parametrize("server_side", [False, True])
def test_typed_map_preserves_datetime64_fraction(server_side):
    type_ = sqla_type_from_name("Map(String, DateTime64(6))")
    value = {"user_1": datetime(2024, 6, 13, 7, 8, 9, 123456)}
    expected = "{'user_1':'2024-06-13 07:08:09.123456'}"
    dialect = ClickHouseDialect(dbapi=dbapi, server_side_params=server_side)
    statement = sa.select(sa.bindparam("p", value, type_))
    compiled = statement.compile(dialect=dialect)
    cursor = Mock()
    previous = common.get_setting("dict_parameter_format")
    common.set_setting("dict_parameter_format", "map")
    try:
        dialect.do_execute(
            cursor, str(compiled), {"p": value}, SimpleNamespace(compiled=compiled, execution_options={}, invoked_statement=statement)
        )
        sql, bound = cursor.execute.call_args.args
        if server_side:
            assert bind_query(sql, bound)[1] == {"param_p": expected}
        else:
            assert finalize_query(sql, bound) == f"SELECT {expected} AS `anon_1`"
        assert str(statement.compile(dialect=dialect, compile_kwargs={"literal_binds": True})) == f"SELECT {expected} AS `anon_1`"
    finally:
        common.set_setting("dict_parameter_format", previous)


def test_named_tuple_expanding_bind():
    dialect = ClickHouseDialect(dbapi=dbapi)
    column = sa.column("value", sqla_type_from_name(NAMED))
    statement = sa.select(column.in_(sa.bindparam("p%value", [VALUE], expanding=True)))
    compiled = statement.compile(dialect=dialect)
    expanded = compiled._process_parameters_for_postcompile(compiled.construct_params(escape_names=False))
    context = SimpleNamespace(
        compiled=compiled, execution_options={}, invoked_statement=statement, _expanded_parameters=expanded.parameter_expansion
    )
    cursor = Mock()
    dialect.do_execute(cursor, expanded.statement, expanded.additional_parameters, context)
    sql, bound = cursor.execute.call_args.args
    assert "(13, 79)" in finalize_query(sql, bound)


@pytest.mark.parametrize("value", [{"protocol": 13}, {**VALUE, "other": 1}])
def test_named_tuple_client_binding_rejects_wrong_keys(value):
    dialect = ClickHouseDialect(dbapi=dbapi)
    statement = sa.select(sa.bindparam("p", value, sqla_type_from_name(NAMED)))
    compiled = statement.compile(dialect=dialect)
    with pytest.raises(ProgrammingError, match="Named Tuple fields do not match"):
        dialect.do_execute(
            Mock(), str(compiled), {"p": value}, SimpleNamespace(compiled=compiled, execution_options={}, invoked_statement=statement)
        )
