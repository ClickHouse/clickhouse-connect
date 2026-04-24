import pytest
from sqlalchemy import Column, MetaData, Table, column, func, select
from sqlalchemy.dialects import registry

# Import sql module so Select.array_join / Select.left_array_join monkey-patches are installed.
import clickhouse_connect.cc_sqlalchemy.sql  # noqa: F401
from clickhouse_connect.cc_sqlalchemy import dialect_name
from clickhouse_connect.cc_sqlalchemy.datatypes.sqltypes import Array, String, UInt32
from clickhouse_connect.cc_sqlalchemy.sql.clauses import ArrayJoin

dialect = registry.load(dialect_name)()
metadata = MetaData()

products = Table(
    "products",
    metadata,
    Column("id", UInt32),
    Column("names", Array(String)),
    Column("prices", Array(UInt32)),
    Column("quantities", Array(UInt32)),
)


def compile_sql(stmt):
    return str(stmt.compile(dialect=dialect, compile_kwargs={"literal_binds": True}))


def test_single_column_array_join():
    stmt = select(products.c.id).array_join(products.c.names)
    sql = compile_sql(stmt)
    assert "ARRAY JOIN" in sql
    assert "LEFT ARRAY JOIN" not in sql
    assert "`names`" in sql
    # The wrapped table should not appear twice in the FROM clause
    assert sql.count("products") == 1 or "FROM products ARRAY JOIN" in sql.replace("`", "")


def test_multi_column_array_join():
    stmt = select(products.c.id).array_join(products.c.names, products.c.prices)
    sql = compile_sql(stmt)
    assert "ARRAY JOIN" in sql
    after_aj = sql.split("ARRAY JOIN")[1]
    assert "`names`" in after_aj
    assert "`prices`" in after_aj
    assert "," in after_aj


def test_left_array_join_single_column():
    stmt = select(products.c.id).left_array_join(products.c.names)
    sql = compile_sql(stmt)
    assert "LEFT ARRAY JOIN" in sql
    assert "`names`" in sql


def test_left_array_join_with_alias():
    stmt = select(products.c.id).left_array_join(products.c.names, alias="tag")
    sql = compile_sql(stmt)
    assert "LEFT ARRAY JOIN" in sql
    assert "AS `tag`" in sql


def test_left_array_join_multi_column_with_alias_list():
    stmt = select(products.c.id).left_array_join(products.c.names, products.c.prices, alias=["n", "p"])
    sql = compile_sql(stmt)
    assert "LEFT ARRAY JOIN" in sql
    after_aj = sql.split("LEFT ARRAY JOIN")[1]
    assert "AS `n`" in after_aj
    assert "AS `p`" in after_aj


def test_chainable_with_final():
    stmt = select(products.c.id).final().left_array_join(products.c.names)
    sql = compile_sql(stmt)
    assert "FINAL" in sql
    assert "LEFT ARRAY JOIN" in sql


def test_chainable_with_final_reverse_order():
    stmt = select(products.c.id).left_array_join(products.c.names).final()
    sql = compile_sql(stmt)
    assert "FINAL" in sql
    assert "LEFT ARRAY JOIN" in sql


def test_array_join_is_generative():
    base = select(products.c.id)
    chained = base.array_join(products.c.names)
    assert base is not chained
    base_sql = compile_sql(base)
    chained_sql = compile_sql(chained)
    assert "ARRAY JOIN" not in base_sql
    assert "ARRAY JOIN" in chained_sql


def test_zero_froms_raises_value_error():
    stmt = select()
    with pytest.raises(ValueError):
        stmt.array_join(products.c.names)


def test_no_columns_raises_value_error():
    stmt = select(products.c.id)
    with pytest.raises(ValueError):
        stmt.array_join()


def test_multi_column_alias_must_be_list_when_multiple_cols():
    stmt = select(products.c.id)
    with pytest.raises(ValueError):
        stmt.array_join(products.c.names, products.c.prices, alias="oops")


def test_array_join_chainable_works_without_explicit_dialect_compile():
    """Regression: the @compiles(ArrayJoin) fallback must load as soon as the
    package is imported, not only when compiler.py is touched. Otherwise
    `select(...).left_array_join(...).final()` raises UnsupportedCompilationError
    because .final() walks the FROM list via StrSQLCompiler.
    """
    stmt = select(products.c.id).left_array_join(products.c.names).final()
    # Must not raise — get_final_froms internally dispatches through the
    # default StrSQLCompiler, which needs the @compiles(ArrayJoin) fallback.
    froms = stmt.get_final_froms()
    assert len(froms) == 1
    # The final-wrapped FROM is still the ArrayJoin (generative copy preserved).

    assert isinstance(froms[0], ArrayJoin)


def test_array_join_preserves_labeled_expressions():
    """A SQLAlchemy Label on an ARRAY JOIN column renders as the ARRAY JOIN alias so
    downstream `column(name)` references bind to the aliased column.
    """
    md = MetaData()
    t = Table("rows", md, Column("id", UInt32), Column("payload", String))
    stmt = select(t.c.id, column("item"), column("item_index")).left_array_join(
        func.JSONExtractArrayRaw(column("payload")).label("item"),
        func.arrayEnumerate(func.JSONExtractArrayRaw(column("payload"))).label("item_index"),
    )
    sql = compile_sql(stmt)
    assert "AS `item`" in sql, f"label dropped: {sql}"
    assert "AS `item_index`" in sql, f"label dropped: {sql}"


def test_array_join_explicit_alias_overrides_label():
    """Explicit alias= argument wins over the expression's own .label()."""
    md = MetaData()
    t = Table("rows2", md, Column("id", UInt32), Column("payload", String))
    stmt = select(t.c.id).left_array_join(
        func.JSONExtractArrayRaw(column("payload")).label("from_label"),
        alias="from_alias_arg",
    )
    sql = compile_sql(stmt)
    assert "AS `from_alias_arg`" in sql
    assert "AS `from_label`" not in sql
