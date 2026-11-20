from io import StringIO
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
import sqlalchemy as sa
from alembic.autogenerate.api import render_python_code
from alembic.operations import Operations, ops
from alembic.runtime.migration import MigrationContext
from sqlalchemy import Column
from sqlalchemy.sql.compiler import GenericTypeCompiler

from clickhouse_connect.cc_sqlalchemy import alembic as ch
from clickhouse_connect.cc_sqlalchemy.alembic.indexes import _aliases
from clickhouse_connect.cc_sqlalchemy.datatypes.sqltypes import String, UInt64
from clickhouse_connect.cc_sqlalchemy.ddl.tableengine import MergeTree
from clickhouse_connect.cc_sqlalchemy.dialect import ClickHouseDialect
from clickhouse_connect.cc_sqlalchemy.sql.compiler import ChStatementCompiler
from clickhouse_connect.driver.binding import format_str, quote_identifier


def _literal_operations(token):
    name = f"item`\\ {token}"
    select = f"SELECT 13 % 5 AS id, {format_str(token)} AS value WHERE 'user_1' LIKE 'user_%'"
    settings = {"custom_setting": token}
    index = ch.ClickHouseIndex(name, f"value LIKE {format_str(token)}", "set(13)")
    projection = ch.ClickHouseProjection(name, select)
    refresh = ch.ClickHouseRefresh(every="1 DAY", depends_on=((name, name),), settings={"custom_setting": token})
    return [
        ch.AddClickHouseIndexOp(name, name, index.expression, index.type_, after_index=name, schema=name),
        ch.AddClickHouseIndexesOp(name, [index], schema=name, clickhouse_settings=settings),
        ch.DropClickHouseIndexOp(name, name, schema=name),
        ch.DropClickHouseIndexesOp(name, [name], schema=name),
        ch.MaterializeClickHouseIndexOp(name, name, partition=format_str(token), schema=name),
        ch.AddClickHouseProjectionOp(name, name, select, after_projection=name, schema=name),
        ch.AddClickHouseProjectionsOp(name, [projection], schema=name, clickhouse_settings=settings),
        ch.DropClickHouseProjectionOp(name, name, schema=name),
        ch.DropClickHouseProjectionsOp(name, [name], schema=name),
        ch.MaterializeClickHouseProjectionOp(name, name, partition=format_str(token), schema=name),
        ch.ModifyClickHouseTableSettingsOp(name, settings, schema=name),
        ch.ResetClickHouseTableSettingsOp(name, ["custom_setting"], schema=name, clickhouse_settings=settings),
        ch.CreateClickHouseViewOp(name, select, schema=name),
        ch.DropClickHouseViewOp(name, schema=name),
        ch.CreateClickHouseFunctionOp(name, f"value -> concat(value, {format_str(token)})"),
        ch.DropClickHouseFunctionOp(name),
        ch.CreateClickHouseMaterializedViewOp(name, name, select, schema=name, to_schema=name, comment=token),
        ch.CreateClickHouseMaterializedViewOp(name, name, select, refresh=refresh, schema=name, to_schema=name, comment=token),
        ch.CreateClickHouseMaterializedViewOp(name, None, select, engine=f"MergeTree ORDER BY {format_str(token)}", comment=token),
        ch.CreateClickHouseMaterializedViewOp(name, None, select, engine=MergeTree(order_by=format_str(token)), refresh=refresh),
        ch.DropClickHouseMaterializedViewOp(name, schema=name, clickhouse_settings=settings),
        ch.ModifyClickHouseMaterializedViewRefreshOp(name, refresh, schema=name),
        ch.ModifyClickHouseMaterializedViewQueryOp(name, select, schema=name),
        ch.CreateClickHouseDictionaryOp(
            name,
            [
                Column(name, UInt64),
                Column("value", String, server_default=token),
                Column("generic", sa.String, server_default=token),
            ],
            primary_key=quote_identifier(name),
            source=f"CLICKHOUSE(QUERY {format_str(select)})",
            layout=f"FLAT(/* {token} */)",
            lifetime=f"MIN 0 MAX 13 /* {token} */",
            schema=name,
            comment=token,
            clickhouse_settings=settings,
        ),
        ch.DropClickHouseDictionaryOp(name, schema=name, clickhouse_settings=settings),
        ch.ReloadClickHouseDictionaryOp(name, schema=name),
    ]


@pytest.mark.parametrize("paramstyle", ["named", "pyformat"])
@pytest.mark.parametrize("token", ["%", "%%", "%s", "%(name)s"])
@pytest.mark.parametrize("case", [pytest.param(i, id=type(op).__name__ + str(i)) for i, op in enumerate(_literal_operations("%%"))])
def test_literal_sql_matches_offline_and_rendered_revision(token, case, paramstyle):
    operation = _literal_operations(token)[case]
    buffer = StringIO()
    dialect = ClickHouseDialect(paramstyle=paramstyle)
    context = MigrationContext.configure(dialect=dialect, opts={"as_sql": True, "output_buffer": buffer})
    Operations(context).invoke(operation)
    offline = buffer.getvalue().strip()
    expected_buffer = StringIO()
    expected_context = MigrationContext.configure(
        dialect=ClickHouseDialect(paramstyle="named"), opts={"as_sql": True, "output_buffer": expected_buffer}
    )
    Operations(expected_context).invoke(operation)
    assert offline == expected_buffer.getvalue().strip()

    if token == "%%":
        code = render_python_code(ops.UpgradeOps([operation]), migration_context=MigrationContext.configure(dialect=dialect))
        rendered = StringIO()
        rendered_context = MigrationContext.configure(dialect=dialect, opts={"as_sql": True, "output_buffer": rendered})
        exec(
            "\n".join(line[4:] for line in code.splitlines() if line.startswith("    ")),
            {
                "op": Operations(rendered_context),
                "sa": sa,
                "MergeTree": MergeTree,
                "UInt64": UInt64,
                "String": String,
                **{name: getattr(ch, name) for name in ch.__all__},
            },
        )
        assert rendered.getvalue() == buffer.getvalue()
    connection = Mock(dialect=ClickHouseDialect(paramstyle=paramstyle))
    Operations(MigrationContext.configure(connection)).invoke(operation)
    connection.exec_driver_sql.assert_called_once_with(offline[:-1].replace("%", "%%"))


@pytest.mark.parametrize("as_sql", [False, True])
@pytest.mark.parametrize("paramstyle", ["named", "pyformat"])
def test_dictionary_preserves_dialect_type_compiler(as_sql, paramstyle):
    class CustomTypeCompiler(GenericTypeCompiler):
        def visit_DATETIME(self, type_, **kwargs):  # noqa: N802
            return "DateTime64(3, 'UTC')"

    class CustomDialect(ClickHouseDialect):
        type_compiler = type_compiler_cls = CustomTypeCompiler

    dialect = CustomDialect(paramstyle=paramstyle)
    buffer = StringIO()
    connection = Mock(dialect=dialect)
    context = MigrationContext.configure(connection, opts={"as_sql": as_sql, "output_buffer": buffer})
    Operations(context).create_clickhouse_dictionary(
        "dictionary%%",
        [Column("stamp%%", sa.DateTime)],
        primary_key="`stamp%%`",
        source="CLICKHOUSE(TABLE 'source')",
        layout="FLAT",
        lifetime="0",
    )
    expected = (
        "CREATE DICTIONARY `dictionary%%` (`stamp%%` DateTime64(3, 'UTC')) "
        "PRIMARY KEY `stamp%%` SOURCE(CLICKHOUSE(TABLE 'source')) LAYOUT(FLAT()) LIFETIME(0)"
    )
    if as_sql:
        assert buffer.getvalue().strip() == expected + ";"
        connection.exec_driver_sql.assert_not_called()
    else:
        connection.exec_driver_sql.assert_called_once_with(expected.replace("%", "%%"))


@pytest.mark.parametrize("paramstyle", ["named", "pyformat"])
@pytest.mark.parametrize("token", ["%", "%%", "%s", "%(name)s"])
def test_offline_dictionary_percent_literals(paramstyle, token):
    buffer = StringIO()
    context = MigrationContext.configure(dialect=ClickHouseDialect(paramstyle=paramstyle), opts={"as_sql": True, "output_buffer": buffer})
    Operations(context).create_clickhouse_dictionary(
        "dictionary%%",
        [Column("value%%", String, server_default=token)],
        primary_key="`value%%`",
        source="CLICKHOUSE(TABLE 'source')",
        layout="FLAT",
        lifetime="0",
    )
    assert buffer.getvalue().strip() == (
        f"CREATE DICTIONARY `dictionary%%` (`value%%` String DEFAULT '{token}') "
        "PRIMARY KEY `value%%` SOURCE(CLICKHOUSE(TABLE 'source')) LAYOUT(FLAT()) LIFETIME(0);"
    )


@pytest.mark.parametrize("required_arg", [False, True])
@pytest.mark.parametrize("as_sql", [False, True])
def test_literal_rendering_preserves_dialect_instance(required_arg, as_sql):
    class InstanceTypeCompiler(GenericTypeCompiler):
        def visit_DATETIME(self, type_, **kwargs):  # noqa: N802
            return self.dialect.timestamp_type

    class InstanceStatementCompiler(ChStatementCompiler):
        def render_literal_value(self, value, type_):
            return super().render_literal_value(self.dialect.literal_prefix + value if isinstance(value, str) else value, type_)

    class InstanceDialect(ClickHouseDialect):
        def __init__(self, **kwargs):
            super().__init__(**kwargs)
            self.timestamp_type = "DateTime64(3, 'UTC')"
            self.literal_prefix = "user_1:"

    class TenantDialect(InstanceDialect):
        def __init__(self, tenant, **kwargs):
            self.tenant = tenant
            super().__init__(**kwargs)

    dialect = TenantDialect(tenant="user_1", paramstyle="pyformat") if required_arg else InstanceDialect(paramstyle="pyformat")
    dialect.type_compiler = InstanceTypeCompiler(dialect)
    dialect.type_compiler_instance = dialect.type_compiler
    dialect.statement_compiler = InstanceStatementCompiler
    # Populate the live dialect's literal processor cache before literal rendering.
    assert str(sa.literal("%%").compile(dialect=dialect, compile_kwargs={"literal_binds": True})) == "'user_1:%%%%'"
    buffer = StringIO()
    connection = Mock(dialect=dialect)
    context = MigrationContext.configure(connection, opts={"as_sql": as_sql, "output_buffer": buffer})
    Operations(context).create_clickhouse_dictionary(
        "dictionary%%",
        [Column("stamp%%", sa.DateTime), Column("label%%", String, server_default="%%")],
        primary_key="`stamp%%`",
        source="CLICKHOUSE(TABLE 'source')",
        layout="FLAT",
        lifetime="0",
    )
    expected = (
        "CREATE DICTIONARY `dictionary%%` (`stamp%%` DateTime64(3, 'UTC'), `label%%` String DEFAULT 'user_1:%%') "
        "PRIMARY KEY `stamp%%` SOURCE(CLICKHOUSE(TABLE 'source')) LAYOUT(FLAT()) LIFETIME(0)"
    )
    if as_sql:
        assert buffer.getvalue().strip() == expected + ";"
    else:
        connection.exec_driver_sql.assert_called_once_with(expected.replace("%", "%%"))
    table = sa.Table(
        "table%%",
        sa.MetaData(),
        Column("label%%", String),
        Column("alias%%", String, clickhousedb_alias=sa.func.concat(sa.column("label%%"), "%%")),
    )
    assert _aliases(SimpleNamespace(dialect=dialect), table) == "WITH concat(`label%%`, 'user_1:%%') AS `alias%%` "
    assert dialect.paramstyle == "pyformat"
    assert str(sa.literal("%%").compile(dialect=dialect, compile_kwargs={"literal_binds": True})) == "'user_1:%%%%'"
