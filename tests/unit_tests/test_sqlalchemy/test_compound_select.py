import pytest
from sqlalchemy import except_, except_all, intersect, intersect_all, literal, select, union, union_all

from clickhouse_connect.cc_sqlalchemy.dialect import ClickHouseDialect


def compile_sql(statement):
    return str(statement.compile(dialect=ClickHouseDialect(), compile_kwargs={"literal_binds": True}))


@pytest.mark.parametrize(
    "operator, keyword",
    [
        pytest.param(union, "UNION DISTINCT", id="union"),
        pytest.param(union_all, "UNION ALL", id="union-all"),
        pytest.param(except_, "EXCEPT DISTINCT", id="except"),
        pytest.param(except_all, "EXCEPT ALL", id="except-all"),
        pytest.param(intersect, "INTERSECT DISTINCT", id="intersect"),
        pytest.param(intersect_all, "INTERSECT ALL", id="intersect-all"),
    ],
)
def test_compound_select_keywords(operator, keyword):
    statement = operator(
        select(literal(13).label("value")),
        select(literal(79).label("value")),
    )

    assert compile_sql(statement) == f"SELECT 13 AS `value` {keyword} SELECT 79 AS `value`"


def test_union_distinct_in_subquery():
    combined = union(
        select(literal(13).label("value")),
        select(literal(79).label("value")),
    ).subquery("selected_values")

    sql = compile_sql(select(combined.c.value))

    assert "(SELECT 13 AS `value` UNION DISTINCT SELECT 79 AS `value`) AS `selected_values`" in sql


@pytest.mark.parametrize(
    "operator, keyword",
    [
        pytest.param(union, "UNION", id="union"),
        pytest.param(except_, "EXCEPT", id="except"),
        pytest.param(intersect, "INTERSECT", id="intersect"),
    ],
)
def test_compound_distinct_is_clickhouse_specific(operator, keyword):
    statement = operator(
        select(literal(13).label("value")),
        select(literal(79).label("value")),
    )

    generic_sql = str(statement.compile(compile_kwargs={"literal_binds": True}))

    assert f" {keyword} " in generic_sql
    assert f"{keyword} DISTINCT" not in generic_sql
