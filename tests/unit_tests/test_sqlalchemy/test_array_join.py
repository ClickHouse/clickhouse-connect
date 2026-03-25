import pytest
import sqlalchemy as db

from clickhouse_connect.cc_sqlalchemy.datatypes.sqltypes import Array, String, UInt32
from clickhouse_connect.cc_sqlalchemy.dialect import ClickHouseDialect
from clickhouse_connect.cc_sqlalchemy.sql.clauses import ArrayJoin, array_join

dialect = ClickHouseDialect()
metadata = db.MetaData()

products = db.Table(
    "products",
    metadata,
    db.Column("id", UInt32),
    db.Column("names", Array(String)),
    db.Column("prices", Array(UInt32)),
    db.Column("quantities", Array(UInt32)),
)


def compile_sql(query):
    return str(query.compile(dialect=dialect, compile_kwargs={"literal_binds": True}))


def test_single_column_no_alias():
    query = db.select(products.c.id, products.c.names).select_from(array_join(products, products.c.names))
    sql = compile_sql(query)
    assert "ARRAY JOIN" in sql
    assert "LEFT" not in sql
    assert "AS" not in sql.split("ARRAY JOIN")[1]


def test_single_column_with_alias():
    query = db.select(products.c.id, db.literal_column("n")).select_from(array_join(products, products.c.names, alias="n"))
    sql = compile_sql(query)
    assert "ARRAY JOIN" in sql
    assert "AS `n`" in sql


def test_single_column_left():
    query = db.select(products.c.id).select_from(array_join(products, products.c.names, is_left=True))
    sql = compile_sql(query)
    assert "LEFT ARRAY JOIN" in sql


def test_multi_column_with_aliases():
    query = db.select(
        products.c.id,
        db.literal_column("item_name"),
        db.literal_column("price"),
        db.literal_column("qty"),
    ).select_from(
        array_join(
            products,
            [products.c.names, products.c.prices, products.c.quantities],
            alias=["item_name", "price", "qty"],
        )
    )
    sql = compile_sql(query)
    after_aj = sql.split("ARRAY JOIN")[1]
    assert "AS `item_name`" in after_aj
    assert "AS `price`" in after_aj
    assert "AS `qty`" in after_aj
    # Columns should be comma-separated
    assert after_aj.count(",") >= 2


def test_multi_column_no_aliases():
    query = db.select(products.c.id, products.c.names, products.c.prices).select_from(
        array_join(
            products,
            [products.c.names, products.c.prices],
        )
    )
    sql = compile_sql(query)
    after_aj = sql.split("ARRAY JOIN")[1]
    assert "AS" not in after_aj
    assert "`names`" in after_aj
    assert "`prices`" in after_aj


def test_multi_column_left():
    query = db.select(products.c.id).select_from(
        array_join(
            products,
            [products.c.names, products.c.prices],
            alias=["n", "p"],
            is_left=True,
        )
    )
    sql = compile_sql(query)
    assert "LEFT ARRAY JOIN" in sql
    assert "AS `n`" in sql
    assert "AS `p`" in sql


def test_multi_column_mixed_aliases():
    """Some columns aliased, some not"""
    query = db.select(
        products.c.id,
        db.literal_column("item_name"),
        products.c.prices,
        db.literal_column("qty"),
    ).select_from(
        array_join(
            products,
            [products.c.names, products.c.prices, products.c.quantities],
            alias=["item_name", None, "qty"],
        )
    )
    sql = compile_sql(query)
    after_aj = sql.split("ARRAY JOIN")[1]
    assert "AS `item_name`" in after_aj
    assert "AS `qty`" in after_aj
    # prices should appear without an alias
    assert "`prices`" in after_aj
    # Make sure there's no AS immediately following prices
    prices_segment = after_aj.split("`prices`")[1].lstrip()
    assert prices_segment.startswith(",")


def test_error_alias_list_with_single_column():
    with pytest.raises(ValueError, match="must be a string or None"):
        array_join(products, products.c.names, alias=["n"])


def test_error_alias_string_with_multi_column():
    with pytest.raises(ValueError, match="must be a list"):
        array_join(products, [products.c.names, products.c.prices], alias="n")


def test_error_alias_length_mismatch():
    with pytest.raises(ValueError, match="must match"):
        array_join(
            products,
            [products.c.names, products.c.prices],
            alias=["n"],
        )


def test_error_empty_column_list():
    with pytest.raises(ValueError, match="At least one"):
        array_join(products, [])


def test_direct_constructor_backward_compat():
    """ArrayJoin is public API. Old-style positional calls must still work."""
    aj = ArrayJoin(products, products.c.names, "n", True)
    query = db.select(products.c.id, db.literal_column("n")).select_from(aj)
    sql = compile_sql(query)
    assert "LEFT ARRAY JOIN" in sql
    assert "AS `n`" in sql


def test_direct_constructor_no_alias():
    """ArrayJoin constructor with no alias, keyword is_left."""
    aj = ArrayJoin(products, products.c.names, is_left=False)
    query = db.select(products.c.id, products.c.names).select_from(aj)
    sql = compile_sql(query)
    assert "ARRAY JOIN" in sql
    assert "AS" not in sql.split("ARRAY JOIN")[1]
