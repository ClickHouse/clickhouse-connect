import pytest
from sqlalchemy import Column, MetaData, Table
from sqlalchemy import column as sa_column
from sqlalchemy import select as sa_select
from sqlalchemy import table as sa_table
from sqlalchemy import values as sa_values
from sqlalchemy.dialects import postgresql, registry
from sqlalchemy.sql.selectable import Values

from clickhouse_connect.cc_sqlalchemy import cte, dialect_name, select
from clickhouse_connect.cc_sqlalchemy.datatypes.sqltypes import String, UInt32

dialect = registry.load(dialect_name)()
metadata = MetaData()
HAS_VALUES_CTE = hasattr(Values, "cte")

book = Table(
    "book",
    metadata,
    Column("book_id", UInt32),
    Column("genre", String),
    Column("score", UInt32),
)


def compile_sql(stmt):
    return str(stmt.compile(dialect=dialect, compile_kwargs={"literal_binds": True}))


def ranked_cte(materialized: bool):
    return select(book.c.book_id, book.c.score).where(book.c.genre == "sci-fi").cte("ranked", materialized=materialized)


def test_cte_materialized_renders_keyword():
    sql = compile_sql(sa_select(ranked_cte(True).c.book_id))
    assert "WITH `ranked` AS MATERIALIZED" in sql


def test_cte_default_is_not_materialized():
    sql = compile_sql(sa_select(ranked_cte(False).c.book_id))
    assert "MATERIALIZED" not in sql
    assert "WITH `ranked` AS" in sql


@pytest.mark.parametrize("materialized", [True, False])
def test_module_level_cte_on_plain_sqlalchemy_select(materialized: bool):
    ranked = cte(sa_select(book.c.book_id), "ranked", materialized=materialized)
    sql = compile_sql(sa_select(ranked.c.book_id))
    assert ("WITH `ranked` AS MATERIALIZED" in sql) is materialized


@pytest.mark.skipif(not HAS_VALUES_CTE, reason="SQLAlchemy Values.cte() is unavailable")
def test_module_level_cte_accepts_a_values_construct():
    """`Values.cte()` is the one non-Select CTE source the dialect already supports."""
    rows = sa_values(sa_column("book_id", UInt32), name="row_source").data([(13,), (17,)])
    sql = compile_sql(sa_select(cte(rows, "ranked", materialized=True).c.book_id))
    # A VALUES CTE renders an explicit column list between the name and the keyword.
    assert "WITH `ranked`(`book_id`) AS MATERIALIZED" in sql


def test_module_level_cte_rejects_a_statement_without_cte_support():
    with pytest.raises(TypeError, match="Got Column"):
        cte(book.c.book_id, "ranked")


def test_keyword_does_not_leak_to_other_dialects():
    ranked = cte(sa_select(book.c.book_id), "ranked", materialized=True)
    sql = str(sa_select(ranked.c.book_id).compile(dialect=postgresql.dialect()))
    assert "MATERIALIZED" not in sql


def test_repeated_references_emit_one_materialized_cte():
    """The late-materialization pattern from issue #900: join plus an IN predicate."""
    ranked = ranked_cte(True)
    stmt = (
        select(book.c.book_id, ranked.c.score)
        .select_from(book)
        .ch_join(ranked, book.c.book_id == ranked.c.book_id, strictness="ANY")
        .where(book.c.book_id.in_(sa_select(ranked.c.book_id)))
    )
    sql = compile_sql(stmt)
    assert sql.count("MATERIALIZED") == 1
    assert "ANY INNER JOIN" in sql


def test_materialized_and_plain_ctes_have_distinct_cache_keys():
    """A materialized CTE must not reuse the compiled statement cached for the plain one."""
    # Untyped core columns so the statements produce a cache key at all.
    lightweight = sa_select(sa_table("book", sa_column("book_id")).c.book_id)
    plain_key = sa_select(cte(lightweight, "ranked").c.book_id)._generate_cache_key()
    materialized_key = sa_select(cte(lightweight, "ranked", materialized=True).c.book_id)._generate_cache_key()
    assert plain_key is not None
    assert materialized_key != plain_key


@pytest.mark.parametrize(
    "build_cte",
    [
        lambda: select(book.c.book_id).cte("ranked", recursive=True, materialized=True),
        lambda: cte(sa_select(book.c.book_id), "ranked", recursive=True, materialized=True),
    ],
)
def test_recursive_materialized_cte_is_rejected(build_cte):
    with pytest.raises(ValueError, match="materialized CTEs cannot be recursive"):
        build_cte()
