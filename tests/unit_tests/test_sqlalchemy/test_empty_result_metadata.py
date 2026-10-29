from datetime import datetime, timedelta, timezone
from decimal import Decimal
from enum import Enum
from types import MethodType, SimpleNamespace
from unittest.mock import Mock, patch

import pytest
import sqlalchemy as sa

from clickhouse_connect.cc_sqlalchemy.datatypes.sqltypes import DateTime64
from clickhouse_connect.cc_sqlalchemy.dialect import _SQLALCHEMY_MAJOR_VERSION, ClickHouseDialect
from clickhouse_connect.cc_sqlalchemy.sql import cte
from clickhouse_connect.dbapi.cursor import Cursor

_DIALECT = ClickHouseDialect()


def _resolves_to_enum(result_type):
    resolved = result_type.dialect_impl(_DIALECT)
    if type(resolved) is sa.Enum:
        return True
    if isinstance(resolved, sa.TypeDecorator):
        impl = resolved.impl_instance if _SQLALCHEMY_MAJOR_VERSION >= 2 else resolved.impl
        return _resolves_to_enum(impl)
    return False


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


class _Status(Enum):
    OPEN = "open"
    CLOSED = "closed"


class _ResultDecorator(sa.TypeDecorator):
    impl = sa.String
    cache_ok = True

    def __init__(self, result_impl=None):
        super().__init__()
        if result_impl is not None:
            self.impl = result_impl

    def process_result_value(self, value, dialect):
        return None if value is None else ("processed", value)


class _DialectResultDecorator(_ResultDecorator):
    def __init__(self, result_impl):
        super().__init__()
        self.result_impl = result_impl

    def load_dialect_impl(self, dialect):
        return self.result_impl


class _DelegatingResultDecorator(sa.TypeDecorator):
    impl = sa.String
    cache_ok = True

    def __init__(self):
        super().__init__()
        self.result_processor = _WrappedArrayResultType().result_processor


class _TypeCodeString(sa.String):
    def result_processor(self, dialect, coltype):
        assert coltype.startswith("Array(")
        return None


class _TypeCodeEnum(sa.Enum, _TypeCodeString):
    pass


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


_SAFE_RESULT_CASES = [
    (_ResultDecorator(), "String", "user_1", ("processed", "user_1")),
    (
        _ResultDecorator(DateTime64(6, "UTC")),
        "DateTime64(6, 'UTC')",
        datetime(2026, 10, 13, 7, 9, 11, 123456, timezone.utc),
        ("processed", datetime(2026, 10, 13, 7, 9, 11, 123456, timezone.utc)),
    ),
    (
        _ResultDecorator(_ResultDecorator(sa.String())),
        "String",
        "user_1",
        ("processed", ("processed", "user_1")),
    ),
    (_DialectResultDecorator(sa.String()), "String", "user_1", ("processed", "user_1")),
    (
        _ResultDecorator(_DialectResultDecorator(sa.String())),
        "String",
        "user_1",
        ("processed", ("processed", "user_1")),
    ),
    (sa.Enum(_Status), "Enum8('OPEN' = 1, 'CLOSED' = 2)", "OPEN", _Status.OPEN),
    (sa.Enum("OPEN", "CLOSED"), "String", "OPEN", "OPEN"),
    (_ResultDecorator(sa.Enum(_Status)), "String", "OPEN", ("processed", _Status.OPEN)),
    (
        sa.String().with_variant(_ResultDecorator(), "clickhousedb"),
        "String",
        "user_1",
        ("processed", "user_1"),
    ),
    (
        _ResultDecorator(sa.String().with_variant(sa.Enum(_Status), "clickhousedb")),
        "String",
        "OPEN",
        ("processed", _Status.OPEN),
    ),
    (
        _DialectResultDecorator(_ArrayResultType().with_variant(sa.String(), "clickhousedb")),
        "String",
        "user_1",
        ("processed", "user_1"),
    ),
    (sa.Boolean(), "Bool", True, True),
    (_ResultDecorator(sa.Boolean()), "Bool", True, ("processed", True)),
    (sa.Numeric(10, 2), "Decimal(10, 2)", Decimal("13.79"), Decimal("13.79")),
    (_ResultDecorator(sa.Numeric(10, 2)), "Decimal(10, 2)", Decimal("13.79"), ("processed", Decimal("13.79"))),
    (sa.Float(), "Float64", 79, 79.0),
    (_ResultDecorator(sa.Float()), "Float64", 79, ("processed", 79.0)),
    (sa.Interval(), "DateTime", datetime(1970, 1, 1, 0, 0, 13), timedelta(seconds=13)),
    (_ResultDecorator(sa.Interval()), "DateTime", datetime(1970, 1, 1, 0, 0, 13), ("processed", timedelta(seconds=13))),
]
# These processors ignore the type code, but the dialect does not decode populated rows for them yet.
_EMPTY_ONLY_RESULT_CASES = [
    (sa.LargeBinary(), "String"),
    (_ResultDecorator(sa.LargeBinary()), "String"),
    (sa.PickleType(), "String"),
    (_ResultDecorator(sa.PickleType()), "String"),
]
if hasattr(sa, "Uuid"):
    _EMPTY_ONLY_RESULT_CASES += [(sa.Uuid(), "UUID"), (_ResultDecorator(sa.Uuid()), "UUID")]


@pytest.mark.parametrize("no_parameters", [False, True])
@pytest.mark.parametrize("empty", [True, False], ids=["empty", "populated"])
@pytest.mark.parametrize("result_type, type_code, value, expected", _SAFE_RESULT_CASES)
def test_safe_result_processors_skip_metadata_probe(no_parameters, empty, result_type, type_code, value, expected):
    if not empty and sa.__version__.startswith("1.4.") and _resolves_to_enum(result_type):
        pytest.skip("SQLAlchemy 1.4 Enum processors require bytes with this dialect's existing Unicode configuration")
    _check_safe_result_processor(no_parameters, empty, result_type, type_code, value, expected)


@pytest.mark.parametrize("no_parameters", [False, True])
@pytest.mark.parametrize("result_type, type_code", _EMPTY_ONLY_RESULT_CASES)
def test_empty_only_safe_result_processors_skip_metadata_probe(no_parameters, result_type, type_code):
    _check_safe_result_processor(no_parameters, True, result_type, type_code, None, None)


def _check_safe_result_processor(no_parameters, empty, result_type, type_code, value, expected):
    client = Mock()
    client.query.return_value = _query_result([(13,)], ["value_1"], [SimpleNamespace(name="UInt64")])
    with patch("clickhouse_connect.dbapi.connection.create_client", return_value=client):
        engine = sa.create_engine("clickhousedb://localhost/default")
        try:
            with engine.connect() as connection:
                connection = connection.execution_options(no_parameters=no_parameters)
                statement = sa.select(sa.column("value_1", result_type))
                client.query.reset_mock()
                if empty:
                    client.query.side_effect = [
                        _query_result(),
                        _query_result(names=["value_1"], types=[SimpleNamespace(name=type_code)]),
                    ]
                    statement = statement.where(sa.false())
                else:
                    client.query.return_value = _query_result([(value,), (None,)], ["value_1"], [SimpleNamespace(name=type_code)])

                result = connection.execute(statement)

                assert list(result.keys()) == ["value_1"]
                if empty:
                    assert result.cursor.description[0][1] is None
                    assert result.cursor.description[0][6] is None
                assert result.all() == ([] if empty else [(expected,), (None,)])
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
@pytest.mark.parametrize(
    "result_type",
    [
        _ArrayResultType(),
        _WrappedArrayResultType(),
        _ArrayResultDecorator(),
        _ResultDecorator(_ArrayResultType()),
        _ResultDecorator(_ResultDecorator(_ArrayResultType())),
        _ResultDecorator(_ArrayResultDecorator()),
        _DialectResultDecorator(_ArrayResultType()),
        _ResultDecorator(_DialectResultDecorator(_ArrayResultType())),
        _ResultDecorator(sa.String().with_variant(_ArrayResultType(), "clickhousedb")),
        sa.String().with_variant(_ArrayResultDecorator(), "clickhousedb"),
        _TypeCodeEnum("OPEN", "CLOSED"),
        _ResultDecorator(_TypeCodeEnum("OPEN", "CLOSED")),
        _DelegatingResultDecorator(),
        _ResultDecorator(_DelegatingResultDecorator()),
    ],
)
def test_empty_custom_result_processor_keeps_server_type_code(no_parameters, result_type):
    _check_custom_result_processor(no_parameters, result_type)


@pytest.mark.parametrize("no_parameters", [False, True])
@pytest.mark.parametrize("receiver", ["same", "different"])
def test_empty_substituted_enum_processor_keeps_server_type_code(no_parameters, receiver):
    result_type = sa.Enum("OPEN", "CLOSED")

    def substitute(engine):
        # Replace the processor on the memoized dialect impl so the compiled statement sees it.
        impl = result_type.dialect_impl(engine.dialect)
        impl.result_processor = (
            MethodType(_TypeCodeString.result_processor, impl) if receiver == "same" else _TypeCodeEnum("OPEN", "CLOSED").result_processor
        )

    _check_custom_result_processor(no_parameters, result_type, substitute)


@pytest.mark.parametrize("no_parameters", [False, True])
def test_empty_replaced_enum_class_processor_keeps_server_type_code(no_parameters):
    with patch.object(sa.Enum, "result_processor", _TypeCodeString.result_processor):
        _check_custom_result_processor(no_parameters, sa.Enum("OPEN", "CLOSED"))


def _check_custom_result_processor(no_parameters, result_type, prepare=None):
    client = Mock()
    client.query.return_value = _query_result([(13,)], ["value_1"], [SimpleNamespace(name="UInt64")])
    with patch("clickhouse_connect.dbapi.connection.create_client", return_value=client):
        engine = sa.create_engine("clickhousedb://localhost/default")
        try:
            if prepare is not None:
                prepare(engine)
            with engine.connect() as connection:
                client.query.reset_mock()
                client.query.side_effect = [
                    _query_result(),
                    _query_result(names=["values_1"], types=[SimpleNamespace(name="Array(UInt64)")]),
                ]

                result = connection.execution_options(no_parameters=no_parameters).execute(sa.select(sa.column("values_1", result_type)))

                assert list(result.keys()) == ["values_1"]
                assert result.cursor.description[0][1] == "Array(UInt64)"
                assert result.cursor.description[0][6] is False
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
