from types import SimpleNamespace
from unittest.mock import Mock, patch

import pytest
import sqlalchemy as sa

from clickhouse_connect.cc_sqlalchemy.dialect import ClickHouseDialect
from clickhouse_connect.cc_sqlalchemy.sql import cte
from clickhouse_connect.dbapi.cursor import Cursor


class _ArrayResultType(sa.types.UserDefinedType):
    cache_ok = True

    def get_col_spec(self, **kwargs):
        return "Array(UInt64)"

    def result_processor(self, dialect, coltype):
        assert coltype.startswith("Array(")
        return None


class _WrappedArrayResultType(sa.TypeDecorator):
    impl = _ArrayResultType
    cache_ok = True


class _ArrayResultDecorator(sa.TypeDecorator):
    impl = sa.String
    cache_ok = True

    def result_processor(self, dialect, coltype):
        assert coltype.startswith("Array(")
        return None


class _NoopResultType(sa.types.UserDefinedType):
    cache_ok = True

    def get_col_spec(self, **kwargs):
        return "UInt64"


def _query_result(rows=(), names=(), types=()):
    return SimpleNamespace(result_set=list(rows), column_names=names, column_types=types, summary={})


@pytest.mark.parametrize("no_parameters", [False, True])
@pytest.mark.parametrize(
    "statement, expected_keys",
    [
        (sa.select(sa.column("value_1")), ["value_1"]),
        (sa.select(sa.column("value_1", sa.Integer)), ["value_1"]),
        (sa.select(sa.column("value_1", sa.String)), ["value_1"]),
        (sa.select(sa.column("value_1", _NoopResultType())), ["value_1"]),
        (sa.select(sa.column("value_1").label("the_value")), ["the_value"]),
        (sa.select(sa.column(sa.sql.quoted_name("value_1", True))), ["value_1"]),
        (sa.select(sa.func.upper(sa.column("label"))), ["upper_1"]),
        (sa.select(sa.literal(13)), ["anon_1"]),
        (sa.select(cte(sa.select(sa.column("value_1")), "values_1", materialized=True)), ["value_1"]),
        (sa.select(sa.select(sa.column("value_1")).prefix_with("/* source */").subquery()), ["value_1"]),
        (sa.select(sa.column("value_1")).with_statement_hint("ignored", dialect_name="_ch_modifier"), ["value_1"]),
        (sa.lambda_stmt(lambda: sa.select(sa.column("value_1"))), ["value_1"]),
        (
            sa.select(sa.column("value_1")).union_all(sa.select(sa.column("value_1"))),
            ["value_1"],
        ),
    ],
)
def test_empty_compiled_result_uses_one_query(statement, expected_keys, no_parameters):
    client = Mock()
    client.query.return_value = _query_result([(13,)], ["value_1"], [SimpleNamespace(name="UInt64")])
    with patch("clickhouse_connect.dbapi.connection.create_client", return_value=client):
        engine = sa.create_engine("clickhousedb://localhost/default")
        try:
            with engine.connect() as connection:
                client.query.reset_mock()
                client.query.side_effect = [
                    _query_result(),
                    _query_result(names=expected_keys, types=[SimpleNamespace(name="UInt64")] * len(expected_keys)),
                ]

                result = connection.execution_options(no_parameters=no_parameters).execute(statement)

                assert list(result.keys()) == expected_keys
                assert result.all() == []
                assert client.query.call_count == 1
        finally:
            engine.dispose()


@pytest.mark.parametrize("execute_no_params", [False, True])
@pytest.mark.parametrize("label_style", [sa.LABEL_STYLE_NONE, sa.LABEL_STYLE_DISAMBIGUATE_ONLY])
def test_empty_self_join_preserves_disambiguated_cursor_names(execute_no_params, label_style):
    source = sa.table("one", sa.column("dummy"), schema="system")
    left = source.alias("a")
    right = source.alias("b")
    statement = (
        sa.select(left.c.dummy, right.c.dummy)
        .select_from(left.join(right, left.c.dummy == right.c.dummy))
        .set_label_style(label_style)
        .where(sa.false())
    )
    dialect = ClickHouseDialect()
    compiled = statement.compile(dialect=dialect)
    context = SimpleNamespace(compiled=compiled, statement=str(compiled), execution_options={})
    client = Mock()
    client.query.side_effect = [
        _query_result(),
        _query_result(names=["dummy", "b.dummy"], types=[SimpleNamespace(name="UInt8")] * 2),
    ]
    cursor = Cursor(client)

    if execute_no_params:
        dialect.do_execute_no_params(cursor, str(compiled), context)
    else:
        dialect.do_execute(cursor, str(compiled), {}, context)

    if label_style == sa.LABEL_STYLE_NONE:
        assert [column[0] for column in cursor.description] == ["dummy", "b.dummy"]
        assert client.query.call_count == 2
    else:
        assert [column[0] for column in cursor.description] == ["dummy", "dummy_1"]
        assert client.query.call_count == 1


@pytest.mark.parametrize("execute_no_params", [False, True])
@pytest.mark.parametrize(
    "statement",
    [
        sa.text("SELECT value_1 WHERE 0"),
        sa.text("SELECT value_1 WHERE 0").columns(value_1=sa.Integer),
        sa.text("SELECT value_1 WHERE 0").columns(sa.column("value_1")),
        sa.select(sa.literal_column("*")),
        sa.select(sa.literal_column("value_1, value_2")),
        sa.select(sa.literal_column("untuple(tuple(13, 79))")),
        sa.select(sa.func.untuple(sa.tuple_(13, 79))),
        sa.select(sa.func.UnTuple(sa.tuple_(13, 79)).label("parts")),
        sa.select(sa.func.COLUMNS("value.*")),
        sa.select(sa.select(sa.func.untuple(sa.tuple_(13, 79))).subquery()),
        sa.select(sa.select(sa.func.COLUMNS("value.*")).subquery()),
        sa.select(sa.column(sa.sql.quoted_name("value_1", False))),
        sa.select(sa.column(sa.sql.quoted_name("value_1, 13 AS value_2", False))),
        sa.select(sa.column(sa.sql.quoted_name("COLUMNS('value.*')", False))),
        sa.select(sa.column("value_1").label(sa.sql.quoted_name("value_1, 13 AS value_2", False))),
        sa.select(sa.column("value_1")).prefix_with("COLUMNS('value.*'),"),
        sa.select(sa.column("value_1")).prefix_with("/* query */"),
        sa.select(sa.column("value_1")).prefix_with("COLUMNS('value.*'),").union_all(sa.select(sa.column("value_1"))),
        sa.select(sa.column("value_1")).suffix_with(", 13 AS value_2 WHERE false"),
        sa.select(sa.column("value_1")).with_statement_hint(", 13 AS value_2 WHERE false"),
        sa.select(sa.column("value_1").op(",")(sa.column("value_2"))),
    ],
)
def test_empty_textual_and_expanding_results_keep_metadata_probe(statement, execute_no_params):
    dialect = ClickHouseDialect()
    compiled = statement.compile(dialect=dialect)
    context = SimpleNamespace(
        compiled=compiled,
        statement=str(compiled),
        execution_options={},
    )
    client = Mock()
    client.query.side_effect = [
        _query_result(),
        _query_result(names=["value_1", "value_2"], types=[SimpleNamespace(name="UInt64")] * 2),
    ]
    cursor = Cursor(client)

    if execute_no_params:
        dialect.do_execute_no_params(cursor, str(compiled), context)
    else:
        dialect.do_execute(cursor, str(compiled), compiled.params, context)

    assert cursor.names == ["value_1", "value_2"]
    assert client.query.call_count == 2


@pytest.mark.parametrize(
    "statement",
    [
        sa.select(sa.column("value_1")).prefix_with("/* query */").self_group(),
        sa.select(sa.column("value_1")).prefix_with("/* query */").limit(1).union_all(sa.select(sa.column("value_1"))),
        sa.select(sa.column("value_1")).prefix_with("/* query */").union_all(sa.select(sa.column("value_1"))).self_group(),
        sa.lambda_stmt(lambda: sa.select(sa.column("value_1")).prefix_with("/* query */")),
    ],
)
def test_grouped_and_lambda_select_prefixes_keep_metadata_fallback(statement):
    dialect = ClickHouseDialect()
    compiled = statement.compile(dialect=dialect)

    assert dialect._ch_result_columns(SimpleNamespace(compiled=compiled)) is None


@pytest.mark.parametrize("no_parameters", [False, True])
@pytest.mark.parametrize("result_type", [_ArrayResultType(), _WrappedArrayResultType(), _ArrayResultDecorator()])
def test_empty_custom_result_processor_keeps_server_type_code(no_parameters, result_type):
    client = Mock()
    client.query.return_value = _query_result([(13,)], ["value_1"], [SimpleNamespace(name="UInt64")])
    with patch("clickhouse_connect.dbapi.connection.create_client", return_value=client):
        engine = sa.create_engine("clickhousedb://localhost/default")
        try:
            with engine.connect() as connection:
                client.query.reset_mock()
                client.query.side_effect = [
                    _query_result(),
                    _query_result(names=["values_1"], types=[SimpleNamespace(name="Array(UInt64)")]),
                ]

                result = connection.execution_options(no_parameters=no_parameters).execute(sa.select(sa.column("values_1", result_type)))

                assert list(result.keys()) == ["values_1"]
                assert result.all() == []
                assert client.query.call_count == 2
        finally:
            engine.dispose()


@pytest.mark.parametrize(
    "context",
    [
        None,
        SimpleNamespace(execution_options={}, compiled=None),
    ],
)
def test_uncompiled_result_keeps_metadata_probe(context):
    client = Mock()
    client.query.return_value = _query_result()

    ClickHouseDialect().do_execute(Cursor(client), "SELECT value_1 WHERE 0", {}, context)

    assert client.query.call_count == 2


@pytest.mark.parametrize("no_parameters", [False, True])
def test_replaced_compiled_statement_keeps_server_metadata(no_parameters):
    client = Mock()
    client.query.return_value = _query_result([(13,)], ["value_1"], [SimpleNamespace(name="UInt64")])
    with patch("clickhouse_connect.dbapi.connection.create_client", return_value=client):
        engine = sa.create_engine("clickhousedb://localhost/default")
        try:
            with engine.connect() as connection:
                client.query.reset_mock()
                client.query.side_effect = [
                    _query_result(),
                    _query_result(names=["value_1", "value_2"], types=[SimpleNamespace(name="UInt64")] * 2),
                ]

                @sa.event.listens_for(connection, "before_cursor_execute", retval=True)
                def replace_statement(conn, cursor, statement, parameters, context, executemany):
                    return "SELECT value_1, value_2 WHERE 0", parameters

                result = connection.execution_options(no_parameters=no_parameters).execute(sa.select(sa.column("value_1")))

                assert list(result.keys()) == ["value_1", "value_2"]
                assert result.all() == []
                assert client.query.call_count == 2
        finally:
            engine.dispose()


@pytest.mark.parametrize("execute_no_params", [False, True])
def test_compiled_statement_preserves_cursor_execute_override(execute_no_params):
    class CustomCursor(Cursor):
        executed = False

        def execute(self, *args, **kwargs):
            self.executed = True
            super().execute(*args, **kwargs)

    dialect = ClickHouseDialect()
    compiled = sa.select(sa.column("value_1")).compile(dialect=dialect)
    context = SimpleNamespace(compiled=compiled, statement=str(compiled), execution_options={})
    client = Mock()
    client.query.return_value = _query_result()
    cursor = CustomCursor(client)

    if execute_no_params:
        dialect.do_execute_no_params(cursor, str(compiled), context)
    else:
        dialect.do_execute(cursor, str(compiled), {}, context)

    assert cursor.executed is True
    assert client.query.call_count == 2


@pytest.mark.parametrize("execute_no_params", [False, True])
@pytest.mark.parametrize("rows", [[], [(13,)]])
def test_compiled_result_with_server_metadata_skips_projection_checks(execute_no_params, rows):
    dialect = ClickHouseDialect()
    compiled = sa.select(sa.column("value_1")).compile(dialect=dialect)
    context = SimpleNamespace(compiled=compiled, statement=str(compiled), execution_options={})
    client = Mock()
    client.query.return_value = _query_result(rows, ["value_1"], [SimpleNamespace(name="UInt64")])
    cursor = Cursor(client)

    with patch.object(dialect, "_ch_result_columns", side_effect=AssertionError("Projection checks must remain lazy")) as helper:
        if execute_no_params:
            dialect.do_execute_no_params(cursor, str(compiled), context)
        else:
            dialect.do_execute(cursor, str(compiled), {}, context)

    assert cursor.types == ["UInt64"]
    helper.assert_not_called()
    client.query.assert_called_once()


@pytest.mark.parametrize("execute_no_params", [False, True])
@pytest.mark.parametrize("wrapper_scope", ["instance", "class"])
def test_compiled_statement_preserves_execute_wrappers(execute_no_params, wrapper_scope):
    dialect = ClickHouseDialect()
    compiled = sa.select(sa.column("value_1")).compile(dialect=dialect)
    context = SimpleNamespace(compiled=compiled, statement=str(compiled), execution_options={})
    client = Mock()
    client.query.return_value = _query_result()
    cursor = Cursor(client)
    if wrapper_scope == "instance":
        wrapper = patch.object(cursor, "execute", wraps=cursor.execute)
    else:
        wrapper = patch.object(Cursor, "execute", autospec=True, side_effect=Cursor.execute)

    with wrapper as execute:
        if execute_no_params:
            dialect.do_execute_no_params(cursor, str(compiled), context)
        else:
            dialect.do_execute(cursor, str(compiled), {}, context)

    execute.assert_called_once()
    assert client.query.call_count == 2
