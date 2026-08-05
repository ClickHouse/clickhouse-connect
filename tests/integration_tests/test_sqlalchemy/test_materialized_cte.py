import pytest
from sqlalchemy import Column, MetaData, Table, func, text
from sqlalchemy import select as sa_select
from sqlalchemy.engine import Engine

from clickhouse_connect.cc_sqlalchemy import cte, select
from clickhouse_connect.cc_sqlalchemy.datatypes.sqltypes import String, UInt32
from tests.integration_tests.test_sqlalchemy.conftest import verify_tables_ready

# Materialization requires the keyword, the setting, and the analyzer.
MATERIALIZED_CTE_SETTINGS = {"enable_materialized_cte": 1, "enable_analyzer": 1}


@pytest.fixture(scope="module", autouse=True)
def materialized_cte_table(test_engine: Engine, test_db: str):
    with test_engine.connect() as conn:
        if not conn.connection.driver_connection.client.min_version("26.3"):
            pytest.skip("Materialized CTEs require ClickHouse 26.3+")

    with test_engine.begin() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS {test_db}.materialized_cte_book"))
        conn.execute(
            text(
                f"""
            CREATE TABLE {test_db}.materialized_cte_book (
                book_id UInt32,
                genre String,
                score UInt32
            ) ENGINE MergeTree() ORDER BY book_id
        """
            )
        )
        conn.execute(
            text(
                f"""
            INSERT INTO {test_db}.materialized_cte_book VALUES
            (13, 'sci-fi', 90),
            (17, 'sci-fi', 70),
            (23, 'sci-fi', 50),
            (29, 'crime', 95)
        """
            )
        )
        verify_tables_ready(conn, {f"{test_db}.materialized_cte_book": 4})


@pytest.fixture(name="book")
def book_fixture(test_db: str) -> Table:
    return Table(
        "materialized_cte_book",
        MetaData(schema=test_db),
        Column("book_id", UInt32),
        Column("genre", String),
        Column("score", UInt32),
    )


def ranked_cte(book: Table, materialized: bool):
    return (
        select(book.c.book_id, func.row_number().over(order_by=book.c.score.desc()).label("result_rank"))
        .where(book.c.genre == "sci-fi")
        .cte("ranked", materialized=materialized)
    )


@pytest.mark.parametrize("materialized", [True, False])
def test_materialized_cte_referenced_twice(test_engine: Engine, book: Table, materialized: bool):
    """The late-materialization pattern from issue #900 returns the same rows either way."""
    ranked = ranked_cte(book, materialized)
    stmt = (
        select(book.c.book_id, ranked.c.result_rank)
        .select_from(book)
        .ch_join(ranked, book.c.book_id == ranked.c.book_id, strictness="ANY")
        .where(book.c.book_id.in_(sa_select(ranked.c.book_id)))
        .order_by(ranked.c.result_rank)
        .execution_options(settings=MATERIALIZED_CTE_SETTINGS)
    )

    assert ("AS MATERIALIZED" in str(stmt.compile(test_engine))) is materialized

    with test_engine.connect() as conn:
        assert conn.execute(stmt).all() == [(13, 1), (17, 2), (23, 3)]


def test_module_level_cte_executes(test_engine: Engine, book: Table):
    """The module-level cte() works on a statement built with plain sqlalchemy.select()."""
    ranked = cte(sa_select(book.c.book_id).where(book.c.genre == "sci-fi"), "ranked", materialized=True)
    stmt = sa_select(func.count()).select_from(ranked).execution_options(settings=MATERIALIZED_CTE_SETTINGS)

    assert "AS MATERIALIZED" in str(stmt.compile(test_engine))

    with test_engine.connect() as conn:
        assert conn.execute(stmt).scalar_one() == 3


@pytest.mark.parametrize(
    "settings",
    [
        {"enable_materialized_cte": 0, "enable_analyzer": 1},
        {"enable_materialized_cte": 1, "enable_analyzer": 0},
    ],
    ids=["setting-disabled", "analyzer-disabled"],
)
def test_disabled_materialized_cte_is_a_silent_no_op(test_engine: Engine, book: Table, settings: dict[str, int]):
    """With either requirement off, the server ignores the keyword instead of erroring.

    The query succeeds and the plan inlines the CTE body, with no
    exception and no warning. Forgetting the setting is a performance bug, not a failure.
    """
    ranked = ranked_cte(book, materialized=True)
    stmt = sa_select(func.count()).select_from(ranked).execution_options(settings=settings)

    with test_engine.connect() as conn:
        assert conn.execute(stmt).scalar_one() == 3
