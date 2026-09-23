import pytest
import sqlalchemy as db
from sqlalchemy import func
from sqlalchemy.sql.functions import GenericFunction

from clickhouse_connect.cc_sqlalchemy.datatypes.sqltypes import DateTime, DateTime64, String, UInt32
from clickhouse_connect.cc_sqlalchemy.dialect import ClickHouseDialect

dialect = ClickHouseDialect()
metadata = db.MetaData()

commits = db.Table(
    "commits",
    metadata,
    db.Column("time", DateTime),
    db.Column("author", String),
    db.Column("lines_added", UInt32),
)


class UncachedGroupFunction(GenericFunction):
    name = "uncached_group_function"


def compile_query(stmt):
    return str(stmt.compile(dialect=dialect, compile_kwargs={"literal_binds": True}))


def test_group_by_renders_label_alias():
    """Labeled expression in GROUP BY should render the alias, not the full expression."""
    time_label = func.toStartOfDay(func.toDateTime(commits.c.time)).label("time")
    stmt = db.select(time_label, func.sum(commits.c.lines_added)).group_by(time_label)
    sql = compile_query(stmt)
    assert "GROUP BY `time`" in sql
    assert "GROUP BY toStartOfDay" not in sql


def test_group_by_multiple_labels():
    """Multiple labeled expressions in GROUP BY should all render as aliases."""
    time_label = func.toStartOfDay(func.toDateTime(commits.c.time)).label("day")
    author_label = func.lower(commits.c.author).label("author_lc")
    stmt = db.select(time_label, author_label, func.sum(commits.c.lines_added)).group_by(time_label, author_label)
    sql = compile_query(stmt)
    assert "GROUP BY `day`, `author_lc`" in sql


def test_group_by_unlabeled_column():
    """Unlabeled columns in GROUP BY should render normally (table-qualified)."""
    stmt = db.select(commits.c.author, func.sum(commits.c.lines_added)).group_by(commits.c.author)
    sql = compile_query(stmt)
    assert "GROUP BY `commits`.`author`" in sql


def test_select_still_renders_full_expression():
    """SELECT clause should still render the full expression AS alias (no regression)."""
    time_label = func.toStartOfDay(func.toDateTime(commits.c.time)).label("time")
    stmt = db.select(time_label, func.sum(commits.c.lines_added)).group_by(time_label)
    sql = compile_query(stmt)
    assert "toStartOfDay(toDateTime(`commits`.`time`)) AS `time`" in sql


def test_order_by_still_renders_alias():
    """ORDER BY should still render the alias (no regression)."""
    time_label = func.toStartOfDay(func.toDateTime(commits.c.time)).label("time")
    stmt = db.select(time_label, func.sum(commits.c.lines_added)).group_by(time_label).order_by(time_label)
    sql = compile_query(stmt)
    assert "ORDER BY `time`" in sql


@pytest.mark.parametrize("name", ["bucket", "day of week", "a`b", None])
@pytest.mark.parametrize("selected", [False, True])
def test_group_by_alias_requires_selected_label(name, selected):
    label = func.lower(commits.c.author).label(name)
    columns = [label, func.count()] if selected else [func.count()]
    stmt = db.select(*columns).select_from(commits).group_by(label)
    sql = compile_query(stmt)
    if selected:
        alias = sql.split(" AS ", 1)[1].split(",", 1)[0]
        assert sql.endswith(f"GROUP BY {alias}")
    else:
        assert sql.endswith("GROUP BY lower(`commits`.`author`)")


@pytest.mark.parametrize("selected", [False, True])
def test_group_by_function_expands_implicit_label(selected):
    expression = func.lower(commits.c.author)
    columns = [expression.label("author_lc"), func.count()] if selected else [func.count()]
    stmt = db.select(*columns).select_from(commits).group_by(expression)
    assert compile_query(stmt).endswith("GROUP BY lower(`commits`.`author`)")


def test_group_by_same_name_requires_matching_expression():
    selected = func.lower(commits.c.author).label("bucket")
    grouped = func.upper(commits.c.author).label("bucket")
    stmt = db.select(selected, func.count()).group_by(grouped)
    assert compile_query(stmt).endswith("GROUP BY upper(`commits`.`author`)")


@pytest.mark.parametrize("rebuild", [False, True])
@pytest.mark.parametrize(
    "expression_factory",
    [
        lambda: func.lower(commits.c.author),
        lambda: func.modulo(commits.c.lines_added, 13),
        lambda: func.modulo(commits.c.lines_added, db.bindparam("value", 13, type_=UInt32())),
    ],
    ids=["function", "literal-bind", "typed-bind"],
)
def test_group_by_equivalent_label_keeps_alias(rebuild, expression_factory):
    label = expression_factory().label("bucket")
    grouped = expression_factory().label("bucket") if rebuild else label._clone()
    stmt = db.select(label, func.count()).group_by(grouped)
    assert compile_query(stmt).endswith("GROUP BY `bucket`")


@pytest.mark.parametrize(
    "grouped_bind", [db.bindparam("value", 79, type_=db.Integer), db.bindparam("value", callable_=lambda: 79, type_=db.Integer)]
)
def test_group_by_alias_rendering_is_stable_for_statement_cache(grouped_bind):
    selected = func.modulo(commits.c.lines_added, db.bindparam("value", 13, type_=db.Integer)).label("bucket")
    original = func.modulo(commits.c.lines_added, db.bindparam("value", 13, type_=db.Integer)).label("bucket")
    varied = func.modulo(commits.c.lines_added, grouped_bind).label("bucket")
    baseline = db.select(selected, func.count()).group_by(original)
    statement = db.select(selected, func.count()).group_by(varied)
    assert baseline._generate_cache_key().key == statement._generate_cache_key().key
    assert str(baseline.compile(dialect=dialect)) == str(statement.compile(dialect=dialect))


@pytest.mark.parametrize(
    "selected, grouped",
    [
        (
            func.modulo(commits.c.lines_added, db.bindparam("selected_value", 13)),
            func.modulo(commits.c.lines_added, db.bindparam("grouped_value", 13)),
        ),
        (func.lower(commits.c.author), func.lower(commits.alias("other").c.author)),
    ],
    ids=["bind-names", "source-alias"],
)
def test_group_by_different_expression_does_not_reuse_alias(selected, grouped):
    stmt = db.select(selected.label("bucket"), func.count()).group_by(grouped.label("bucket"))
    assert not compile_query(stmt).endswith("GROUP BY `bucket`")


def test_group_by_callable_binds_reuse_alias_without_evaluation():
    calls = []

    def value():
        calls.append(None)
        return 13

    selected = func.modulo(commits.c.lines_added, db.bindparam("value", callable_=value)).label("bucket")
    grouped = func.modulo(commits.c.lines_added, db.bindparam("value", callable_=value)).label("bucket")
    sql = str(db.select(selected, func.count()).group_by(grouped).compile(dialect=dialect))
    assert sql.endswith("GROUP BY `bucket`")
    assert calls == []


@pytest.mark.filterwarnings("error::sqlalchemy.exc.SAWarning")
@pytest.mark.parametrize("type_factory", [db.DateTime, lambda: DateTime64(3)])
def test_group_by_rebuilt_cast_keeps_alias(type_factory):
    selected = db.cast(db.column("value"), type_factory()).label("bucket")
    grouped = db.cast(db.column("value"), type_factory()).label("bucket")
    assert compile_query(db.select(selected, func.count()).group_by(grouped)).endswith("GROUP BY `bucket`")


@pytest.mark.parametrize("kind", ["dict", "custom", "numpy-array", "numpy-scalar"])
def test_group_by_non_scalar_binds_do_not_compare_values(kind):
    class NoEquality:
        def __eq__(self, other):
            raise AssertionError("Bound values must not be compared")

    if kind.startswith("numpy"):
        np = pytest.importorskip("numpy")
        left, right = (np.array([13, 79]), np.array([13, 77])) if kind == "numpy-array" else (np.int64(13), np.int64(79))
    elif kind == "dict":
        left, right = {"value": NoEquality()}, {"value": NoEquality()}
    else:
        left, right = NoEquality(), NoEquality()
    selected = func.identity(db.bindparam("value", left)).label("bucket")
    grouped = func.identity(db.bindparam("value", right)).label("bucket")
    sql = str(db.select(selected, func.count()).group_by(grouped).compile(dialect=dialect))
    assert sql.endswith("GROUP BY `bucket`")


@pytest.mark.filterwarnings("error::sqlalchemy.exc.SAWarning")
@pytest.mark.parametrize("kind", ["function", "type", "array-type"])
def test_group_by_rebuilt_expression_does_not_generate_cache_warnings(kind):
    class UncachedType(db.TypeDecorator):
        impl = db.String

    if kind == "function":
        selected = UncachedGroupFunction(commits.c.author)
        grouped = UncachedGroupFunction(commits.c.author)
    else:
        type_ = UncachedType() if kind == "type" else db.ARRAY(UncachedType())
        grouped_type = UncachedType() if kind == "type" else db.ARRAY(UncachedType())
        selected = func.identity(commits.c.author, type_=type_)
        grouped = func.identity(commits.c.author, type_=grouped_type)
    statement = db.select(selected.label("bucket"), func.count()).group_by(grouped.label("bucket"))
    assert compile_query(statement).endswith("GROUP BY `bucket`")


@pytest.mark.parametrize("selected", [False, True])
@pytest.mark.parametrize("wrapper", ["function", "cast", "case", "binary"])
def test_group_by_nested_labels_expand(wrapper, selected):
    label = commits.c.author.label("hidden")
    expressions = {
        "function": (func.lower(label), "lower(`commits`.`author`)"),
        "cast": (db.cast(label, db.String), "CAST(`commits`.`author` AS VARCHAR)"),
        "case": (db.case((label == "user_1", 13), else_=79), "CASE WHEN (`commits`.`author` = 'user_1') THEN 13 ELSE 79 END"),
        "binary": (label.concat("x"), "`commits`.`author` || 'x'"),
    }
    expression, expected = expressions[wrapper]
    columns = [label, func.count()] if selected else [func.count()]
    stmt = db.select(*columns).select_from(commits).group_by(expression)
    assert compile_query(stmt).endswith(f"GROUP BY {expected}")


def test_group_by_label_nested_in_selected_expression_expands():
    label = commits.c.author.label("hidden")
    stmt = db.select(func.lower(label), func.count()).group_by(label)
    assert compile_query(stmt).endswith("GROUP BY `commits`.`author`")


def test_group_by_nested_select_has_its_own_label_scope():
    outer_label = commits.c.lines_added.label("outer_label")
    inner_label = commits.c.author.label("inner_label")
    inner = (
        db.select(inner_label)
        .where(outer_label > 13)
        .group_by(inner_label)
        .having(outer_label > 79)
        .order_by(outer_label)
        .scalar_subquery()
    )
    stmt = db.select(outer_label, func.count()).group_by(outer_label, inner)
    sql = compile_query(stmt)
    assert "GROUP BY `outer_label`, (SELECT `commits`.`author` AS `inner_label`" in sql
    assert "WHERE `commits`.`lines_added` > 13 GROUP BY `inner_label`" in sql
    assert "HAVING `commits`.`lines_added` > 79 ORDER BY `commits`.`lines_added`" in sql


def test_group_by_does_not_change_window_or_order_by_labels():
    selected = commits.c.author.label("selected")
    unselected = commits.c.lines_added.label("unselected")
    stmt = (
        db.select(selected, func.count().over(partition_by=selected, order_by=unselected)).group_by(selected).order_by(selected, unselected)
    )
    sql = compile_query(stmt)
    assert "OVER (PARTITION BY `commits`.`author` ORDER BY `commits`.`lines_added`)" in sql
    assert "GROUP BY `selected` ORDER BY `selected`, `commits`.`lines_added`" in sql


def test_group_by_textual_label_reference():
    label = func.lower(commits.c.author).label("bucket")
    stmt = db.select(label, func.count()).group_by("bucket")
    assert compile_query(stmt).endswith("GROUP BY `bucket`")
    with pytest.raises(db.exc.CompileError, match="Can't resolve label reference"):
        compile_query(db.select(func.count()).select_from(commits).group_by("missing"))
