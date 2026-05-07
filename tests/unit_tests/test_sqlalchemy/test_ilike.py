from sqlalchemy import column, table

from clickhouse_connect import dbapi
from clickhouse_connect.cc_sqlalchemy.dialect import ClickHouseDialect


def _compile_clause(clause):
    return str(clause.compile(dialect=ClickHouseDialect(dbapi=dbapi)))


def test_ilike_compiles_to_clickhouse_ilike():
    users = table("users", column("name"))

    assert _compile_clause(users.c.name.ilike("%user_1%")) == "`users`.`name` ILIKE %(name_1)s"


def test_not_ilike_compiles_to_clickhouse_not_ilike():
    users = table("users", column("name"))

    assert _compile_clause(users.c.name.not_ilike("%user_1%")) == "`users`.`name` NOT ILIKE %(name_1)s"
