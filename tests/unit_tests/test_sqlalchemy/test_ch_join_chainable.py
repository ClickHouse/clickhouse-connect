import pytest
import sqlalchemy as db
from sqlalchemy import select
from sqlalchemy.dialects import registry
from sqlalchemy.orm import declarative_base

# Import sql module so Select.ch_join monkey-patch is installed.
import clickhouse_connect.cc_sqlalchemy.sql  # noqa: F401
from clickhouse_connect.cc_sqlalchemy import dialect_name
from clickhouse_connect.cc_sqlalchemy.datatypes.sqltypes import String, UInt32
from clickhouse_connect.cc_sqlalchemy.sql.clauses import ch_join

dialect = registry.load(dialect_name)()
metadata = db.MetaData()

books = db.Table(
    "books",
    metadata,
    db.Column("id", UInt32),
    db.Column("author_id", UInt32),
    db.Column("publisher_id", UInt32),
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

reviews = db.Table(
    "reviews",
    metadata,
    db.Column("id", UInt32),
    db.Column("book_id", UInt32),
)


def compile_sql(stmt):
    return str(stmt.compile(dialect=dialect, compile_kwargs={"literal_binds": True}))


@pytest.mark.parametrize(
    "kwargs,expected",
    [
        ({"strictness": "ALL"}, "ALL INNER JOIN"),
        ({"strictness": "ANY"}, "ANY INNER JOIN"),
        ({"strictness": "ASOF"}, "ASOF INNER JOIN"),
        ({"isouter": True, "strictness": "SEMI"}, "SEMI LEFT OUTER JOIN"),
        ({"isouter": True, "strictness": "ANTI"}, "ANTI LEFT OUTER JOIN"),
        ({"distribution": "GLOBAL"}, "GLOBAL INNER JOIN"),
        ({"full": True, "strictness": "ALL"}, "ALL FULL OUTER JOIN"),
    ],
)
def test_single_ch_join_modifiers(kwargs, expected):
    stmt = select(books.c.id, authors.c.name).select_from(books).ch_join(authors, authors.c.id == books.c.author_id, **kwargs)
    sql = compile_sql(stmt)
    assert expected in sql


def test_single_ch_join_cross():
    stmt = select(books.c.id, authors.c.name).select_from(books).ch_join(authors, cross=True)
    sql = compile_sql(stmt)
    assert "CROSS JOIN" in sql
    assert " ON " not in sql


def test_single_ch_join_using():
    stmt = select(books.c.id, authors.c.name).select_from(books).ch_join(authors, using=["id"])
    sql = compile_sql(stmt)
    assert "INNER JOIN" in sql
    assert "USING (`id`)" in sql
    assert " ON " not in sql


def test_two_join_chain_renders_both_with_single_from():
    stmt = (
        select(authors.c.name, publishers.c.name)
        .select_from(books)
        .ch_join(authors, authors.c.id == books.c.author_id, isouter=True, strictness="ANY")
        .ch_join(publishers, publishers.c.id == books.c.publisher_id, isouter=True, strictness="ANY")
    )
    sql = compile_sql(stmt)
    assert sql.count("ANY LEFT OUTER JOIN") == 2
    assert sql.count("FROM") == 1
    assert "FROM `books` ANY LEFT OUTER JOIN" in sql


def test_three_join_chain_renders_all_with_single_from():
    stmt = (
        select(authors.c.name, publishers.c.name, reviews.c.id)
        .select_from(books)
        .ch_join(authors, authors.c.id == books.c.author_id, strictness="ALL")
        .ch_join(publishers, publishers.c.id == books.c.publisher_id, strictness="ALL")
        .ch_join(reviews, reviews.c.book_id == books.c.id, strictness="ALL")
    )
    sql = compile_sql(stmt)
    assert sql.count("ALL INNER JOIN") == 3
    assert sql.count("FROM") == 1
    assert "FROM `books` ALL INNER JOIN `authors`" in sql
    assert "`publishers`" in sql
    assert "`reviews`" in sql


def test_ch_join_is_generative():
    base = select(books.c.id, authors.c.name).select_from(books)
    chained = base.ch_join(authors, authors.c.id == books.c.author_id, strictness="ALL")
    assert base is not chained
    base_sql = compile_sql(base)
    chained_sql = compile_sql(chained)
    assert "JOIN" not in base_sql
    assert "ALL INNER JOIN" in chained_sql


def test_ch_join_state_survives_where():
    stmt = (
        select(authors.c.name, publishers.c.name)
        .select_from(books)
        .ch_join(authors, authors.c.id == books.c.author_id, strictness="ANY")
        .where(books.c.id > 13)
        .ch_join(publishers, publishers.c.id == books.c.publisher_id, strictness="ALL")
    )
    sql = compile_sql(stmt)
    assert "ANY INNER JOIN" in sql
    assert "ALL INNER JOIN" in sql


def test_ch_join_after_final():
    stmt = select(books.c.id).select_from(books).final().ch_join(authors, authors.c.id == books.c.author_id, strictness="ALL")
    sql = compile_sql(stmt)
    assert "FINAL" in sql
    assert "ALL INNER JOIN" in sql


def test_final_after_ch_join():
    # After a join the single FROM is the join itself, so final() needs an explicit
    # target table to disambiguate, same as the existing module-level ch_join + final.
    stmt = (
        select(books.c.id, authors.c.name)
        .select_from(books)
        .ch_join(authors, authors.c.id == books.c.author_id, strictness="ALL")
        .final(books)
    )
    sql = compile_sql(stmt)
    assert "FINAL" in sql
    assert "ALL INNER JOIN" in sql


def test_ch_join_parity_with_module_factory():
    chained = select(authors.c.name).select_from(books).ch_join(authors, authors.c.id == books.c.author_id, isouter=True, strictness="ANY")
    j = ch_join(books, authors, authors.c.id == books.c.author_id, isouter=True, strictness="ANY")
    explicit = select(authors.c.name).select_from(j)
    assert compile_sql(chained) == compile_sql(explicit)


def test_ch_join_single_from_no_select_from():
    stmt = select(books.c.id).ch_join(authors, authors.c.id == books.c.author_id, strictness="ALL")
    sql = compile_sql(stmt)
    assert "ALL INNER JOIN" in sql
    assert "FROM `books` ALL INNER JOIN" in sql


def test_ch_join_ambiguous_left_raises():
    stmt = select(authors.c.name, publishers.c.name).select_from(books).select_from(authors)
    with pytest.raises(ValueError, match="cannot determine the left side"):
        stmt.ch_join(publishers, publishers.c.id == books.c.publisher_id)


def test_ch_join_invalid_strictness_raises():
    stmt = select(books.c.id).select_from(books)
    with pytest.raises(ValueError, match="Invalid strictness"):
        stmt.ch_join(authors, authors.c.id == books.c.author_id, strictness="PARTIAL")


def test_ch_join_mixed_with_native_join_raises():
    stmt = select(books.c.id).select_from(books).join(authors, authors.c.id == books.c.author_id)
    with pytest.raises(ValueError, match="native .join.. on the same statement"):
        stmt.ch_join(publishers, publishers.c.id == books.c.publisher_id)


Base = declarative_base()


class AuthorEntity(Base):
    __tablename__ = "author_entities"
    id = db.Column(UInt32, primary_key=True)
    name = db.Column(String)


class BookEntity(Base):
    __tablename__ = "book_entities"
    id = db.Column(UInt32, primary_key=True)
    author_id = db.Column(UInt32)


def test_ch_join_orm_renders():
    stmt = (
        select(AuthorEntity.name, BookEntity.id)
        .select_from(AuthorEntity)
        .ch_join(BookEntity, BookEntity.author_id == AuthorEntity.id, isouter=True, strictness="ANY")
    )
    sql = compile_sql(stmt)
    assert "ANY LEFT OUTER JOIN" in sql
    assert "`book_entities`" in sql


# Cache-key tests select only UInt32 columns. UInt32 inherits SQLAlchemy Integer
# (cache_ok = True) so _generate_cache_key() returns a real, non-None key. A String
# column (UserDefinedType, cache_ok unset) would suppress the key to None and make
# these assertions vacuous, also emitting an SAWarning.


def test_ch_join_cache_key_identical_chains_match():
    a = select(books.c.id).select_from(books).ch_join(authors, authors.c.id == books.c.author_id, strictness="ALL")
    b = select(books.c.id).select_from(books).ch_join(authors, authors.c.id == books.c.author_id, strictness="ALL")
    key_a = a._generate_cache_key()
    key_b = b._generate_cache_key()
    assert key_a is not None
    assert key_a == key_b


def test_ch_join_cache_key_differs_on_strictness():
    a = select(books.c.id).select_from(books).ch_join(authors, authors.c.id == books.c.author_id, strictness="ALL")
    b = select(books.c.id).select_from(books).ch_join(authors, authors.c.id == books.c.author_id, strictness="ANY")
    key_a = a._generate_cache_key()
    key_b = b._generate_cache_key()
    assert key_a is not None and key_b is not None
    assert key_a != key_b


def test_ch_join_cache_key_differs_on_right_table():
    a = select(books.c.id).select_from(books).ch_join(authors, authors.c.id == books.c.author_id, strictness="ALL")
    b = select(books.c.id).select_from(books).ch_join(publishers, publishers.c.id == books.c.publisher_id, strictness="ALL")
    key_a = a._generate_cache_key()
    key_b = b._generate_cache_key()
    assert key_a is not None and key_b is not None
    assert key_a != key_b


def test_ch_join_cache_key_differs_on_distribution():
    a = select(books.c.id).select_from(books).ch_join(authors, authors.c.id == books.c.author_id, strictness="ALL")
    b = select(books.c.id).select_from(books).ch_join(authors, authors.c.id == books.c.author_id, strictness="ALL", distribution="GLOBAL")
    key_a = a._generate_cache_key()
    key_b = b._generate_cache_key()
    assert key_a is not None and key_b is not None
    assert key_a != key_b


def test_ch_join_cache_key_chained_matches_nested_factory():
    chained = select(books.c.id).select_from(books).ch_join(authors, authors.c.id == books.c.author_id, strictness="ANY")
    nested = select(books.c.id).select_from(ch_join(books, authors, authors.c.id == books.c.author_id, strictness="ANY"))
    key_chained = chained._generate_cache_key()
    key_nested = nested._generate_cache_key()
    assert key_chained is not None
    assert key_chained == key_nested


def test_ch_join_cache_key_bare_matches_explicit_select_from():
    bare = select(books.c.id).ch_join(authors, authors.c.id == books.c.author_id, strictness="ANY")
    explicit = select(books.c.id).select_from(books).ch_join(authors, authors.c.id == books.c.author_id, strictness="ANY")
    key_bare = bare._generate_cache_key()
    key_explicit = explicit._generate_cache_key()
    assert key_bare is not None
    assert key_bare == key_explicit
