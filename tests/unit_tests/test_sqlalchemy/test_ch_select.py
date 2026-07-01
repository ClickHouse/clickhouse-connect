import warnings

import pytest
import sqlalchemy as db
from sqlalchemy import select as sa_select
from sqlalchemy.dialects import registry

# Import sql module so the Select monkey-patches are installed for the SA path.
import clickhouse_connect.cc_sqlalchemy.sql  # noqa: F401
from clickhouse_connect.cc_sqlalchemy import ClickHouseSelect, dialect_name
from clickhouse_connect.cc_sqlalchemy import select as ch_select
from clickhouse_connect.cc_sqlalchemy.datatypes.sqltypes import String, UInt32

dialect = registry.load(dialect_name)()
metadata = db.MetaData()

books = db.Table(
    "books",
    metadata,
    db.Column("id", UInt32),
    db.Column("author_id", UInt32),
    db.Column("publisher_id", UInt32),
    db.Column("active", UInt32),
)

authors = db.Table(
    "authors",
    metadata,
    db.Column("id", UInt32),
    db.Column("name", String),
)

publishers = db.Table(
    "publishers",
    metadata,
    db.Column("id", UInt32),
    db.Column("name", String),
)


def compile_sql(stmt):
    return str(stmt.compile(dialect=dialect, compile_kwargs={"literal_binds": True}))


# (name, ch-path builder, sa-path builder) for each chainable. The sa-path calls the
# runtime monkey-patch, which is intentionally not statically typed, hence type: ignore.
PARITY_CASES = [
    (
        "final",
        lambda: ch_select(books.c.id).select_from(books).final(),
        lambda: sa_select(books.c.id).select_from(books).final(),  # type: ignore[attr-defined]
    ),
    (
        "sample",
        lambda: ch_select(books.c.id).select_from(books).sample(0.1),
        lambda: sa_select(books.c.id).select_from(books).sample(0.1),  # type: ignore[attr-defined]
    ),
    (
        "array_join",
        lambda: ch_select(books.c.id).select_from(books).array_join(books.c.author_id),
        lambda: sa_select(books.c.id).select_from(books).array_join(books.c.author_id),  # type: ignore[attr-defined]
    ),
    (
        "left_array_join",
        lambda: ch_select(books.c.id).select_from(books).left_array_join(books.c.author_id),
        lambda: sa_select(books.c.id).select_from(books).left_array_join(books.c.author_id),  # type: ignore[attr-defined]
    ),
    (
        "prewhere",
        lambda: ch_select(books.c.id).select_from(books).prewhere(books.c.active == 1),
        lambda: sa_select(books.c.id).select_from(books).prewhere(books.c.active == 1),  # type: ignore[attr-defined]
    ),
    (
        "limit_by",
        lambda: ch_select(books.c.id).select_from(books).limit_by([books.c.author_id], 5),
        lambda: sa_select(books.c.id).select_from(books).limit_by([books.c.author_id], 5),  # type: ignore[attr-defined]
    ),
    (
        "ch_join",
        lambda: (
            ch_select(books.c.id, authors.c.name)
            .select_from(books)
            .ch_join(authors, authors.c.id == books.c.author_id, isouter=True, strictness="ANY")
        ),
        lambda: (
            sa_select(books.c.id, authors.c.name)
            .select_from(books)
            .ch_join(authors, authors.c.id == books.c.author_id, isouter=True, strictness="ANY")  # type: ignore[attr-defined]
        ),
    ),
]


@pytest.mark.parametrize("name,ch_build,sa_build", PARITY_CASES, ids=[c[0] for c in PARITY_CASES])
def test_subclass_parity_with_monkeypatched_select(name, ch_build, sa_build):
    assert compile_sql(ch_build()) == compile_sql(sa_build())


def test_subclass_class_preserved_through_mixed_chain():
    base = ch_select(books.c.id, authors.c.name)
    assert isinstance(base, ClickHouseSelect)

    with_from = base.select_from(books)
    assert isinstance(with_from, ClickHouseSelect)

    joined = with_from.ch_join(authors, authors.c.id == books.c.author_id, strictness="ALL")
    assert isinstance(joined, ClickHouseSelect)

    filtered = joined.where(books.c.id > 13)
    assert isinstance(filtered, ClickHouseSelect)

    final_stmt = filtered.final(books)
    assert isinstance(final_stmt, ClickHouseSelect)

    sql = compile_sql(final_stmt)
    assert "ALL INNER JOIN" in sql
    assert "FINAL" in sql


def test_subclass_compiles_without_warning():
    stmt = ch_select(books.c.id).select_from(books).final().prewhere(books.c.active == 1).limit_by([books.c.author_id], 3)
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        compile_sql(stmt)


def test_subclass_generates_cache_key():
    stmt = ch_select(books.c.id).select_from(books).final()
    key = stmt._generate_cache_key()
    assert key is not None


def test_subclass_multi_join_matches_monkeypatched_chain():
    chained = (
        ch_select(authors.c.name, publishers.c.name)
        .select_from(books)
        .ch_join(authors, authors.c.id == books.c.author_id, isouter=True, strictness="ANY")
        .ch_join(publishers, publishers.c.id == books.c.publisher_id, isouter=True, strictness="ANY")
    )
    monkeypatched = (
        sa_select(authors.c.name, publishers.c.name)
        .select_from(books)
        .ch_join(authors, authors.c.id == books.c.author_id, isouter=True, strictness="ANY")  # type: ignore[attr-defined]
        .ch_join(publishers, publishers.c.id == books.c.publisher_id, isouter=True, strictness="ANY")
    )
    assert compile_sql(chained) == compile_sql(monkeypatched)
