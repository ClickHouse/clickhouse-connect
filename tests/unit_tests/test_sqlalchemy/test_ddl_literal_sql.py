from io import StringIO
from types import SimpleNamespace

import pytest
import sqlalchemy as sa
from alembic.operations import Operations
from alembic.runtime.migration import MigrationContext

from clickhouse_connect.cc_sqlalchemy import ClickHouseIndex, ClickHouseProjection
from clickhouse_connect.cc_sqlalchemy.datatypes.base import sqla_type_from_name
from clickhouse_connect.cc_sqlalchemy.datatypes.sqltypes import String, UInt64
from clickhouse_connect.cc_sqlalchemy.ddl.dictionary import Dictionary
from clickhouse_connect.cc_sqlalchemy.ddl.tableengine import MergeTree
from clickhouse_connect.cc_sqlalchemy.dialect import ClickHouseDialect
from clickhouse_connect.cc_sqlalchemy.sql.ddlcompiler import ChDDLCompiler


@pytest.mark.parametrize("token", ["%", "%%", "%s", "%(name)s"])
@pytest.mark.parametrize("mode", ["named", "pyformat", "server"])
def test_create_table_literal_fragments(token, mode):
    engine = MergeTree(
        order_by=f"concat(value, '{token}')",
        partition_by=sa.func.concat(sa.column("value"), sa.literal(token)),
        primary_key=f"concat(value, '{token}')",
        sample_by=f"cityHash64('{token}')",
        ttl=f"stamp + INTERVAL 13 DAY /* {token} */",
        settings={"storage_policy": token},
    )
    table = sa.Table(
        f"table{token}",
        sa.MetaData(),
        sa.Column(f"value{token}", String, server_default=token, comment=token, clickhousedb_codec=f"ZSTD(1) /* {token} */"),
        sa.Column("alias", String, clickhousedb_alias=sa.func.concat(sa.column(f"value{token}"), token)),
        sa.Column("materialized", String, clickhousedb_materialized=sa.literal(token)),
        sa.Column("expiry", UInt64, clickhousedb_ttl=sa.text(f"stamp /* {token} */")),
        ClickHouseIndex(f"index{token}", f"value LIKE '{token}'", f"set(13) /* {token} */"),
        ClickHouseProjection(f"projection{token}", f"SELECT concat(value, '{token}') AS value"),
        schema=f"db{token}",
        comment=token,
        clickhousedb_engine=engine,
    )
    expected = (
        f"CREATE TABLE `db{token}`.`table{token}` ("
        f"`value{token}` String DEFAULT '{token}' COMMENT '{token}' CODEC(ZSTD(1) /* {token} */), "
        f"`alias` String ALIAS concat(`value{token}`, '{token}'), "
        f"`materialized` String MATERIALIZED '{token}', `expiry` UInt64 TTL stamp /* {token} */, "
        f"INDEX `index{token}` value LIKE '{token}' TYPE set(13) /* {token} */, "
        f"PROJECTION `projection{token}` (SELECT concat(value, '{token}') AS value)) {engine.compile()} COMMENT '{token}'"
    )
    dialect = ClickHouseDialect(paramstyle="named" if mode == "named" else "pyformat", server_side_params=mode == "server")
    rendered = str(sa.schema.CreateTable(table).compile(dialect=dialect))
    assert rendered == (expected.replace("%", "%%") if mode == "pyformat" else expected)
    buffer = StringIO()
    context = MigrationContext.configure(dialect=dialect, opts={"as_sql": True, "output_buffer": buffer})
    Operations(context).impl.create_table(table)
    assert buffer.getvalue().strip() == expected + ";"


@pytest.mark.parametrize("token", ["%", "%%", "%s", "%(name)s"])
@pytest.mark.parametrize("mode", ["named", "pyformat", "server"])
def test_create_dictionary_literal_fragments(token, mode):
    table = Dictionary(
        f"dictionary{token}",
        sa.MetaData(),
        sa.Column(f"key{token}", String, server_default=token, comment=token),
        primary_key=f"`key{token}`",
        source=f"CLICKHOUSE(QUERY 'SELECT {token}')",
        layout=f"FLAT(/* {token} */)",
        lifetime=f"0 /* {token} */",
        comment=token,
    )
    expected = (
        f"CREATE DICTIONARY `dictionary{token}` (`key{token}` String DEFAULT '{token}' COMMENT '{token}') "
        f"PRIMARY KEY `key{token}` SOURCE(CLICKHOUSE(QUERY 'SELECT {token}')) "
        f"LAYOUT(FLAT(/* {token} */)) LIFETIME(0 /* {token} */) COMMENT '{token}'"
    )
    dialect = ClickHouseDialect(paramstyle="named" if mode == "named" else "pyformat", server_side_params=mode == "server")
    assert str(sa.schema.CreateTable(table).compile(dialect=dialect)) == (expected.replace("%", "%%") if mode == "pyformat" else expected)


def test_ddl_identifier_and_column_fragments():
    dialect = ClickHouseDialect(paramstyle="pyformat")
    table = sa.Table("table%%", sa.MetaData(), sa.Column("value%%", String, server_default="%%"), MergeTree(order_by="tuple()"))
    compiler = ChDDLCompiler(dialect, None)

    assert str(sa.schema.CreateSchema("db%%").compile(dialect=dialect)) == "CREATE DATABASE `db%%%%`"
    assert str(sa.schema.DropSchema("db%%").compile(dialect=dialect)) == "DROP DATABASE `db%%%%`"
    assert str(sa.schema.DropTable(table).compile(dialect=dialect)) == "DROP TABLE `table%%%%`"
    assert compiler.visit_add_column(SimpleNamespace(element=table, column=table.c["value%%"])) == (
        "ALTER TABLE `table%%%%` ADD COLUMN `value%%%%` String DEFAULT '%%%%'"
    )
    assert compiler.visit_drop_column(SimpleNamespace(element=table, column=table.c["value%%"])) == (
        "ALTER TABLE `table%%%%` DROP COLUMN `value%%%%`"
    )


@pytest.mark.parametrize(
    "type_name",
    [
        "Enum8('label%%' = 13)",
        "Array(Enum8('label%%' = 13))",
        "Tuple(Enum8('label%%' = 13), String)",
        "Array(Tuple(Enum8('label%%' = 13), String))",
        "Nullable(Enum8('label%%' = 13))",
    ],
)
def test_ddl_literal_type_names(type_name):
    column_type = sqla_type_from_name(type_name)
    table = sa.Table("table", sa.MetaData(), sa.Column("value", column_type), MergeTree(order_by="tuple()"))
    assert str(sa.schema.CreateTable(table).compile(dialect=ClickHouseDialect(paramstyle="pyformat"))) == (
        f"CREATE TABLE `table` (`value` {column_type.name.replace('%', '%%')}) Engine MergeTree ORDER BY tuple()"
    )


def test_dictionary_numeric_lifetime():
    table = Dictionary("dictionary", sa.MetaData(), sa.Column("key", UInt64), lifetime=13)
    assert str(sa.schema.CreateTable(table).compile(dialect=ClickHouseDialect(paramstyle="pyformat"))) == (
        "CREATE DICTIONARY `dictionary` (`key` UInt64) LIFETIME(13)"
    )
