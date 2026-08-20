import warnings

import pytest
import sqlalchemy as sa
from sqlalchemy.dialects import registry
from sqlalchemy.exc import SAWarning

# Import sql so the Select monkey-patches are installed.
import clickhouse_connect.cc_sqlalchemy.sql  # noqa: F401
from clickhouse_connect.cc_sqlalchemy import dialect_name
from clickhouse_connect.cc_sqlalchemy.datatypes.sqltypes import UInt32

dialect = registry.load(dialect_name)()
metadata = sa.MetaData()

events = sa.Table(
    "events",
    metadata,
    sa.Column("id", UInt32),
    sa.Column("user_id", UInt32),
    sa.Column("active", UInt32),
)

users = sa.Table(
    "users",
    metadata,
    sa.Column("id", UInt32),
)


def compile_sql(stmt):
    return str(stmt.compile(dialect=dialect, compile_kwargs={"literal_binds": True}))


@pytest.mark.parametrize("hint_dialect", ["*", "clickhousedb"])
def test_applicable_table_hint_warns(hint_dialect):
    stmt = sa.select(events.c.id).with_hint(events, "USE INDEX event_idx", dialect_name=hint_dialect)

    with pytest.warns(SAWarning, match=r"Select\.with_hint\(\) has no effect") as caught:
        sql = compile_sql(stmt)

    assert len(caught) == 1
    assert sql == "SELECT `events`.`id` \nFROM `events`"


def test_foreign_dialect_table_hint_is_silent():
    stmt = sa.select(events.c.id).with_hint(events, "USE INDEX event_idx", dialect_name="postgresql")

    with warnings.catch_warnings():
        warnings.simplefilter("error")
        sql = compile_sql(stmt)

    assert sql == "SELECT `events`.`id` \nFROM `events`"


def test_multiple_table_hints_warn_once_per_compilation():
    stmt = (
        sa.select(events.c.id, users.c.id)
        .select_from(events.join(users, events.c.user_id == users.c.id))
        .with_hint(events, "event hint")
        .with_hint(users, "user hint", dialect_name="clickhousedb")
    )

    with pytest.warns(SAWarning, match=r"Select\.with_hint\(\) has no effect") as caught:
        first_sql = compile_sql(stmt)
        second_sql = compile_sql(stmt)

    assert len(caught) == 2
    assert first_sql == second_sql
    assert "event hint" not in first_sql
    assert "user hint" not in first_sql


def test_raw_tail_statement_hint_is_unchanged_and_silent():
    stmt = sa.select(events.c.id).with_statement_hint("SETTINGS max_threads=1")

    with warnings.catch_warnings():
        warnings.simplefilter("error")
        sql = compile_sql(stmt)

    assert sql == "SELECT `events`.`id` \nFROM `events` SETTINGS max_threads=1"


def test_clickhouse_select_modifiers_are_silent():
    stmt = sa.select(events.c.id).final().sample(0.1).prewhere(events.c.active == 1).limit_by([events.c.user_id], 3)

    with warnings.catch_warnings():
        warnings.simplefilter("error")
        sql = compile_sql(stmt)

    assert "FROM `events` FINAL SAMPLE 0.1" in sql
    assert "PREWHERE `events`.`active` = 1" in sql
    assert "LIMIT 3 BY `events`.`user_id`" in sql
