import pytest
from sqlalchemy import Column, MetaData, Table, select
from sqlalchemy.dialects import registry

# Import sql module so Select.prewhere / Select.limit_by monkey-patches are installed.
import clickhouse_connect.cc_sqlalchemy.sql  # noqa: F401
from clickhouse_connect.cc_sqlalchemy import dialect_name
from clickhouse_connect.cc_sqlalchemy.datatypes.sqltypes import Bool, String, UInt32

dialect = registry.load(dialect_name)()
metadata = MetaData()

events = Table(
    "events",
    metadata,
    Column("id", UInt32),
    Column("user_id", UInt32),
    Column("active", Bool),
    Column("name", String),
)


def compile_sql(stmt):
    return str(stmt.compile(dialect=dialect, compile_kwargs={"literal_binds": True}))


def test_single_prewhere_no_where():
    stmt = select(events).prewhere(events.c.id == 1)
    sql = compile_sql(stmt)
    assert "PREWHERE" in sql
    assert "WHERE" not in sql.replace("PREWHERE", "")


def test_prewhere_before_where():
    stmt = select(events).where(events.c.id > 10).prewhere(events.c.active == True)  # noqa: E712
    sql = compile_sql(stmt)
    assert "PREWHERE" in sql
    assert "WHERE" in sql.replace("PREWHERE", "")
    # PREWHERE must appear before the standalone WHERE in the rendered text.
    prewhere_idx = sql.index("PREWHERE")
    # Find the WHERE that is not part of PREWHERE.
    stripped = sql.replace("PREWHERE", " " * len("PREWHERE"))
    where_idx = stripped.index("WHERE")
    assert prewhere_idx < where_idx


def test_chained_prewheres_compose_with_and():
    stmt = select(events).prewhere(events.c.id == 1).prewhere(events.c.active == True)  # noqa: E712
    sql = compile_sql(stmt)
    assert "PREWHERE" in sql
    # Both predicates must appear, joined by AND.
    prewhere_section = sql.split("PREWHERE", 1)[1]
    assert "AND" in prewhere_section
    assert "`id`" in prewhere_section
    assert "`active`" in prewhere_section


def test_prewhere_before_group_by_and_having():
    stmt = (
        select(events.c.user_id)
        .prewhere(events.c.active == True)  # noqa: E712
        .group_by(events.c.user_id)
        .having(events.c.user_id > 0)
    )
    sql = compile_sql(stmt)
    assert "PREWHERE" in sql
    assert "GROUP BY" in sql
    assert "HAVING" in sql
    prewhere_idx = sql.index("PREWHERE")
    group_by_idx = sql.index("GROUP BY")
    having_idx = sql.index("HAVING")
    assert prewhere_idx < group_by_idx < having_idx


def test_prewhere_before_order_by_and_limit():
    stmt = (
        select(events)
        .prewhere(events.c.active == True)  # noqa: E712
        .order_by(events.c.id)
        .limit(50)
    )
    sql = compile_sql(stmt)
    assert "PREWHERE" in sql
    assert "ORDER BY" in sql
    assert "LIMIT" in sql
    prewhere_idx = sql.index("PREWHERE")
    order_by_idx = sql.index("ORDER BY")
    limit_idx = sql.index("LIMIT")
    assert prewhere_idx < order_by_idx < limit_idx


def test_single_limit_by():
    stmt = select(events).limit_by([events.c.user_id], 5)
    sql = compile_sql(stmt)
    assert "LIMIT 5 BY" in sql
    assert "`user_id`" in sql


def test_limit_by_with_offset():
    stmt = select(events).limit_by([events.c.user_id], 5, offset=2)
    sql = compile_sql(stmt)
    assert "LIMIT 2, 5 BY" in sql


def test_limit_by_multiple_columns():
    stmt = select(events).limit_by([events.c.user_id, events.c.active], 3)
    sql = compile_sql(stmt)
    assert "LIMIT 3 BY" in sql
    by_section = sql.split("LIMIT 3 BY", 1)[1]
    assert "`user_id`" in by_section
    assert "`active`" in by_section


def test_limit_by_before_regular_limit():
    stmt = select(events).limit_by([events.c.user_id], 5).limit(100)
    sql = compile_sql(stmt)
    limit_by_idx = sql.index("LIMIT 5 BY")
    # The plain LIMIT 100 must appear AFTER the LIMIT BY clause.
    limit_100_idx = sql.index("LIMIT 100")
    assert limit_by_idx < limit_100_idx


def test_chainable_composition_with_final_sample():
    stmt = select(events).final().prewhere(events.c.id == 1).limit_by([events.c.user_id], 3).limit(100)
    sql = compile_sql(stmt)
    assert "FINAL" in sql
    assert "PREWHERE" in sql
    assert "LIMIT 3 BY" in sql
    assert "LIMIT 100" in sql
    # ClickHouse-grammar order: FROM -> FINAL -> PREWHERE -> LIMIT BY -> LIMIT.
    final_idx = sql.index("FINAL")
    prewhere_idx = sql.index("PREWHERE")
    limit_by_idx = sql.index("LIMIT 3 BY")
    limit_100_idx = sql.index("LIMIT 100")
    assert final_idx < prewhere_idx < limit_by_idx < limit_100_idx


def test_prewhere_is_generative():
    base = select(events)
    chained = base.prewhere(events.c.id == 1)
    assert base is not chained
    base_sql = compile_sql(base)
    chained_sql = compile_sql(chained)
    assert "PREWHERE" not in base_sql
    assert "PREWHERE" in chained_sql


def test_limit_by_is_generative():
    base = select(events)
    chained = base.limit_by([events.c.user_id], 5)
    assert base is not chained
    base_sql = compile_sql(base)
    chained_sql = compile_sql(chained)
    assert "LIMIT 5 BY" not in base_sql
    assert "LIMIT 5 BY" in chained_sql


def test_limit_by_empty_raises_value_error():
    stmt = select(events)
    with pytest.raises(ValueError):
        stmt.limit_by([], 5)


def test_prewhere_on_non_select_raises_type_error():
    from clickhouse_connect.cc_sqlalchemy.sql import prewhere

    with pytest.raises(TypeError):
        prewhere("not a select", events.c.id == 1)


def test_limit_by_on_non_select_raises_type_error():
    from clickhouse_connect.cc_sqlalchemy.sql import limit_by

    with pytest.raises(TypeError):
        limit_by("not a select", [events.c.user_id], 5)


def test_prewhere_on_select_from_subquery_does_not_leak_into_inner():
    """
    Regression: the PREWHERE splice must be paren-depth-aware. A naive
    string-splice finds the first WHERE marker anywhere in the text, which
    matches the inner subquery's WHERE and produces invalid SQL with
    PREWHERE injected inside the subquery parens referencing the outer alias.
    """
    subq = select(events.c.id).where(events.c.id > 5).subquery()
    stmt = select(subq.c.id).prewhere(subq.c.id == 1)
    sql = compile_sql(stmt)

    # Outer PREWHERE must attach to the outer SELECT, after the subquery
    # parens close. Inner WHERE must remain inside the parens untouched.
    close_paren = sql.find(") AS")
    assert close_paren != -1, f"expected subquery closing paren in: {sql}"
    assert "PREWHERE" in sql[close_paren:], f"PREWHERE should appear in outer select, not inside subquery: {sql}"
    assert "PREWHERE" not in sql[:close_paren], f"PREWHERE must not be spliced into the inner subquery: {sql}"


def test_prewhere_cache_key_is_stable_for_equivalent_statements():
    """
    Regression: the with_statement_hint key used to force cache-key
    differentiation must be derived from the whereclause's structural
    content, not id(). Otherwise two identical statements get distinct
    cache keys and compiled SQL is never reused.
    """
    stmt_a = select(events.c.id).prewhere(events.c.id == 1)
    stmt_b = select(events.c.id).prewhere(events.c.id == 1)
    key_a = stmt_a._generate_cache_key()
    key_b = stmt_b._generate_cache_key()
    assert key_a is not None and key_b is not None
    assert key_a.key == key_b.key, "equivalent prewhere statements must share a cache key"


def test_limit_by_cache_key_is_stable_for_equivalent_statements():
    """Same regression as above for limit_by: stable cache key, not id()."""
    stmt_a = select(events.c.id).limit_by([events.c.user_id], 5)
    stmt_b = select(events.c.id).limit_by([events.c.user_id], 5)
    key_a = stmt_a._generate_cache_key()
    key_b = stmt_b._generate_cache_key()
    assert key_a is not None and key_b is not None
    assert key_a.key == key_b.key, "equivalent limit_by statements must share a cache key"


def test_prewhere_cache_key_differs_for_different_whereclause_shapes():
    """
    Sanity check: the stable key must still discriminate structurally
    different clauses (different operators / columns / shapes). Literal
    value differences alone SHOULD share a key because SA's str() for
    BinaryExpression uses bind placeholders, so the compiled SQL is the
    same and bind values differ at execute time.
    """
    # Same shape (`id = :bind`), different literals -> share a key.
    eq_1 = select(events.c.id).prewhere(events.c.id == 1)
    eq_2 = select(events.c.id).prewhere(events.c.id == 2)
    assert eq_1._generate_cache_key().key == eq_2._generate_cache_key().key

    # Different shape (`id = :bind` vs `id > :bind`) -> different keys.
    eq_op = select(events.c.id).prewhere(events.c.id == 1)
    gt_op = select(events.c.id).prewhere(events.c.id > 1)
    assert eq_op._generate_cache_key().key != gt_op._generate_cache_key().key

    # Different column -> different keys.
    by_id = select(events.c.id).prewhere(events.c.id == 1)
    by_user = select(events.c.id).prewhere(events.c.user_id == 1)
    assert by_id._generate_cache_key().key != by_user._generate_cache_key().key
