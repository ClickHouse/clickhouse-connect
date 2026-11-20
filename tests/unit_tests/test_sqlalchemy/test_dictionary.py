from io import StringIO
from unittest.mock import Mock

import pytest
import sqlalchemy as sa
from alembic.operations import Operations
from alembic.runtime.migration import MigrationContext

from clickhouse_connect.cc_sqlalchemy import alembic as ch  # noqa: F401
from clickhouse_connect.cc_sqlalchemy.datatypes.sqltypes import String, UInt64
from clickhouse_connect.cc_sqlalchemy.ddl.dictionary import Dictionary
from clickhouse_connect.cc_sqlalchemy.dialect import ClickHouseDialect


@pytest.mark.parametrize("execution", ["dictionary", "table", "offline", "online"])
@pytest.mark.parametrize(
    "option,value",
    [
        ("comment", "label"),
        ("clickhousedb_codec", "ZSTD"),
        ("clickhouse_codec", "ZSTD"),
        ("clickhousedb_ttl", sa.text("id + INTERVAL 1 DAY")),
        ("clickhousedb_materialized", sa.text("13")),
        ("clickhousedb_alias", sa.text("13")),
    ],
)
def test_dictionary_rejects_table_column_options(execution, option, value):
    column = sa.Column("value", String, **{option: value})
    dialect = ClickHouseDialect()
    buffer = StringIO()
    connection = Mock(dialect=dialect)
    if execution in ("dictionary", "table"):
        if execution == "dictionary":
            table = Dictionary("lookup", sa.MetaData(), column)
        else:
            table = sa.Table("lookup", sa.MetaData(), column, clickhousedb_table_type="dictionary")

        def invoke():
            return sa.schema.CreateTable(table).compile(dialect=dialect)

    else:
        context = MigrationContext.configure(connection, opts={"as_sql": execution == "offline", "output_buffer": buffer})

        def invoke():
            return Operations(context).create_clickhouse_dictionary(
                "lookup",
                [column],
                primary_key="value",
                source="CLICKHOUSE(TABLE 'source')",
                layout="FLAT",
                lifetime="0",
            )

    with pytest.raises(sa.exc.CompileError, match=f"Dictionary 'lookup' column 'value'.*{option.split('_')[-1].upper()}"):
        invoke()
    connection.exec_driver_sql.assert_not_called()
    assert buffer.getvalue() == ""


def test_dictionary_select_compiles():
    dictionary = Dictionary("lookup", sa.MetaData(), sa.Column("id", UInt64), sa.Column("value", String))
    assert str(sa.select(dictionary).compile(dialect=ClickHouseDialect())) == "SELECT `lookup`.`id`, `lookup`.`value` \nFROM `lookup`"


@pytest.mark.parametrize("execution", ["compile", "offline"])
def test_dictionary_default_allowed(execution):
    columns = [sa.Column("id", UInt64), sa.Column("value", String, server_default="user_1")]
    dialect = ClickHouseDialect()
    if execution == "compile":
        dictionary = Dictionary("lookup", sa.MetaData(), *columns)
        sql = str(sa.schema.CreateTable(dictionary).compile(dialect=dialect))
    else:
        buffer = StringIO()
        context = MigrationContext.configure(dialect=dialect, opts={"as_sql": True, "output_buffer": buffer})
        Operations(context).create_clickhouse_dictionary(
            "lookup", columns, primary_key="id", source="CLICKHOUSE(TABLE 'source')", layout="FLAT", lifetime="0"
        )
        sql = buffer.getvalue()
    assert "`value` String DEFAULT 'user_1'" in sql
