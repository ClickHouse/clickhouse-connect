import pytest
from sqlalchemy import column, table
from sqlalchemy.exc import CompileError

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


def test_like_compiles_without_escape_clause():
    users = table("users", column("name"))

    assert _compile_clause(users.c.name.like("%user_1%")) == "`users`.`name` LIKE %(name_1)s"


@pytest.mark.parametrize(
    ("clause_name", "operator_name"),
    (
        ("like", "LIKE"),
        ("not_like", "LIKE"),
        ("ilike", "ILIKE"),
        ("not_ilike", "ILIKE"),
    ),
)
def test_like_escape_clause_raises_compile_error(clause_name, operator_name):
    users = table("users", column("name"))
    clause = getattr(users.c.name, clause_name)("%!_%", escape="!")

    with pytest.raises(CompileError, match=f"ClickHouse does not support the ESCAPE clause on {operator_name}"):
        _compile_clause(clause)
