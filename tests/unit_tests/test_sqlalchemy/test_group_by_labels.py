import sqlalchemy as db
from sqlalchemy import func

from clickhouse_connect.cc_sqlalchemy.datatypes.sqltypes import String, UInt32, DateTime
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
    stmt = (
        db.select(time_label, author_label, func.sum(commits.c.lines_added))
        .group_by(time_label, author_label)
    )
    sql = compile_query(stmt)
    assert "GROUP BY `day`, `author_lc`" in sql


def test_group_by_unlabeled_column():
    """Unlabeled columns in GROUP BY should render normally (table-qualified)."""
    stmt = (
        db.select(commits.c.author, func.sum(commits.c.lines_added))
        .group_by(commits.c.author)
    )
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
    stmt = (
        db.select(time_label, func.sum(commits.c.lines_added))
        .group_by(time_label)
        .order_by(time_label)
    )
    sql = compile_query(stmt)
    assert "ORDER BY `time`" in sql
