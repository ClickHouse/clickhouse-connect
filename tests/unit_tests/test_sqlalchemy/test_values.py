from datetime import datetime

import pytest
import sqlalchemy as db
from sqlalchemy import DateTime as SqlaDateTime

from clickhouse_connect.cc_sqlalchemy.datatypes.sqltypes import DateTime
from clickhouse_connect.cc_sqlalchemy.dialect import ClickHouseDialect

SA_2 = db.__version__ >= "2"

dialect = ClickHouseDialect()


def compile_query(stmt):
    return str(stmt.compile(dialect=dialect, compile_kwargs={"literal_binds": True}))


def test_values_renders_clickhouse_table_function_syntax():
    values_clause = db.values(
        db.column("id", db.Integer),
        db.column("name", db.String),
        name="v",
    ).data([(13, "user_1"), (29, "user_2")])

    sql = compile_query(db.select(values_clause))

    assert "FROM VALUES('id Int32, name String', (13, 'user_1'), (29, 'user_2')) AS `v`" in sql
    assert "FROM (VALUES" not in sql
    assert "AS `v` (`id`, `name`)" not in sql


def test_values_escapes_structure_literal_for_clickhouse_type_names():
    values_clause = db.values(
        db.column("ts", DateTime("UTC")),
        name="v",
    ).data([("2024-01-02 03:04:05",)])

    sql = compile_query(db.select(values_clause))

    assert "VALUES('ts DateTime(''UTC'')', ('2024-01-02 03:04:05')) AS `v`" in sql


@pytest.mark.skipif(not SA_2, reason="SA 1.4 lacks literal datetime rendering for this type")
def test_values_maps_generic_sqla_datetime_type():
    values_clause = db.values(
        db.column("ts", SqlaDateTime()),
        name="v",
    ).data([(datetime(2024, 1, 2, 3, 4, 5),)])

    sql = compile_query(db.select(values_clause))

    assert "VALUES('ts DateTime', ('2024-01-02 03:04:05')) AS `v`" in sql


@pytest.mark.skipif(not SA_2, reason="Values.cte() was added in SA 2.x")
def test_values_cte_wraps_table_function_in_select():
    values_clause = (
        db.values(
            db.column("id", db.Integer),
            name="v",
        )
        .data([(17,), (29,)])
        .cte("input_rows")
    )

    sql = compile_query(db.select(values_clause.c.id).select_from(values_clause))

    assert "WITH `input_rows`(`id`) AS" in sql
    assert "(SELECT * FROM VALUES('id Int32', (17), (29)))" in sql
