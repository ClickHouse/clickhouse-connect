import pytest
from sqlalchemy import DateTime, String, case, cast, column, func, literal, literal_column, select, text

from clickhouse_connect.cc_sqlalchemy.datatypes.sqltypes import DateTime64, Decimal
from clickhouse_connect.cc_sqlalchemy.dialect import ClickHouseDialect


@pytest.mark.parametrize(
    "grouping, expected",
    [
        ("function", [(3,), (3,), (4,)]),
        ("label", [(3,), (3,), (4,)]),
        ("selected_function", [(0, 4), (1, 3), (2, 3)]),
        ("nested_label", [(1,), (9,)]),
        ("nested_label_collision", [(1,), (9,)]),
        ("unrelated_label_collision", [(13, 3), (13, 3), (13, 4)]),
    ],
)
def test_group_by_expressions(param_client, call, grouping, expected):
    source_columns = [literal_column("number").label("n")]
    if grouping == "nested_label_collision":
        source_columns.append(literal(13).label("x"))
    source = select(*source_columns).select_from(text("numbers(10)")).subquery("source")
    expression = func.modulo(source.c.n, 3)
    selected = [func.count()]
    if grouping == "label":
        expression = expression.label("bucket")
    elif grouping == "selected_function":
        selected.insert(0, expression.label("bucket"))
    elif grouping.startswith("nested_label"):
        expression = case((cast(source.c.n.label("x"), String) == "1", 13), else_=79)
    elif grouping == "unrelated_label_collision":
        selected.insert(0, literal(13).label("bucket"))
        expression = expression.label("bucket")

    statement = select(*selected).select_from(source).group_by(expression)
    sql = str(statement.compile(dialect=ClickHouseDialect(), compile_kwargs={"literal_binds": True}))
    assert sorted(call(param_client.query, sql).result_rows) == expected


@pytest.mark.parametrize("rebuild", [False, True])
@pytest.mark.parametrize("expression", ["function", "datetime-cast", "datetime64-cast"])
def test_group_by_selected_alias_with_source_column_collision(param_client, call, rebuild, expression):
    source = (
        select((func.toDateTime("2026-09-01 00:00:00") + literal_column("number") * 3600).label("time"))
        .select_from(text("numbers(24)"))
        .subquery("source")
    )

    def time_label():
        value = column("time")
        if expression == "function":
            value = func.toDateTime(value)
        else:
            value = cast(value, DateTime() if expression == "datetime-cast" else DateTime64(3))
        return func.toStartOfDay(value).label("time")

    label = time_label()
    group_label = time_label() if rebuild else label
    grouped = select(label, func.count().label("total")).select_from(source).group_by(group_label).subquery("grouped")
    statement = select(func.toString(grouped.c.time), grouped.c.total)
    sql = str(statement.compile(dialect=ClickHouseDialect(), compile_kwargs={"literal_binds": True}))
    result = call(param_client.query, sql, settings={"prefer_column_name_to_alias": 0})
    assert result.result_rows == [("2026-09-01 00:00:00", 24)]


@pytest.mark.parametrize("difference, expected", [("bind-value", [(0, 4), (1, 3), (2, 3)]), ("type-scale", [(i, 1) for i in range(10)])])
def test_group_by_rebuilt_label_preserves_alias_with_parameter_differences(param_client, call, difference, expected):
    source = select(literal_column("number").label("n")).select_from(text("numbers(10)")).subquery("source")
    if difference == "bind-value":
        selected, grouped = func.modulo(source.c.n, 3), func.modulo(source.c.n, 4)
    else:
        selected, grouped = cast(source.c.n, Decimal(10, 0)), cast(source.c.n, Decimal(10, 1))
    statement = select(selected.label("bucket"), func.count()).group_by(grouped.label("bucket"))
    sql = str(statement.compile(dialect=ClickHouseDialect(), compile_kwargs={"literal_binds": True}))
    assert sorted(call(param_client.query, sql).result_rows) == expected
