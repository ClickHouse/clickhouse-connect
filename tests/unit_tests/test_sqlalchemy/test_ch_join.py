import pytest
import sqlalchemy as db

from clickhouse_connect.cc_sqlalchemy.datatypes.sqltypes import String, UInt32
from clickhouse_connect.cc_sqlalchemy.dialect import ClickHouseDialect
from clickhouse_connect.cc_sqlalchemy.sql.clauses import ch_join

dialect = ClickHouseDialect()
metadata = db.MetaData()

users = db.Table(
    "users",
    metadata,
    db.Column("id", UInt32),
    db.Column("name", String),
)

orders = db.Table(
    "orders",
    metadata,
    db.Column("id", UInt32),
    db.Column("user_id", UInt32),
    db.Column("product", String),
)

items = db.Table(
    "items",
    metadata,
    db.Column("id", UInt32),
    db.Column("order_id", UInt32),
    db.Column("sku", String),
)


def compile_query(stmt):
    return str(stmt.compile(dialect=dialect, compile_kwargs={"literal_binds": True}))


def test_all_inner_join():
    j = ch_join(users, orders, users.c.id == orders.c.user_id, strictness="ALL")
    sql = compile_query(db.select(users.c.name).select_from(j))
    assert "ALL INNER JOIN" in sql


def test_any_inner_join():
    j = ch_join(users, orders, users.c.id == orders.c.user_id, strictness="ANY")
    sql = compile_query(db.select(users.c.name).select_from(j))
    assert "ANY INNER JOIN" in sql


def test_any_left_outer_join():
    j = ch_join(users, orders, users.c.id == orders.c.user_id, isouter=True, strictness="ANY")
    sql = compile_query(db.select(users.c.name).select_from(j))
    assert "ANY LEFT OUTER JOIN" in sql


def test_asof_inner_join():
    j = ch_join(users, orders, users.c.id == orders.c.user_id, strictness="ASOF")
    sql = compile_query(db.select(users.c.name).select_from(j))
    assert "ASOF INNER JOIN" in sql


def test_asof_left_outer_join():
    j = ch_join(users, orders, users.c.id == orders.c.user_id, isouter=True, strictness="ASOF")
    sql = compile_query(db.select(users.c.name).select_from(j))
    assert "ASOF LEFT OUTER JOIN" in sql


def test_semi_left_outer_join():
    j = ch_join(users, orders, users.c.id == orders.c.user_id, isouter=True, strictness="SEMI")
    sql = compile_query(db.select(users.c.name).select_from(j))
    assert "SEMI LEFT OUTER JOIN" in sql


def test_anti_left_outer_join():
    j = ch_join(users, orders, users.c.id == orders.c.user_id, isouter=True, strictness="ANTI")
    sql = compile_query(db.select(users.c.name).select_from(j))
    assert "ANTI LEFT OUTER JOIN" in sql


def test_all_full_outer_join():
    j = ch_join(users, orders, users.c.id == orders.c.user_id, full=True, strictness="ALL")
    sql = compile_query(db.select(users.c.name).select_from(j))
    assert "ALL FULL OUTER JOIN" in sql


def test_global_inner_join():
    j = ch_join(users, orders, users.c.id == orders.c.user_id, distribution="GLOBAL")
    sql = compile_query(db.select(users.c.name).select_from(j))
    assert "GLOBAL INNER JOIN" in sql


def test_global_only_join():
    """GLOBAL without strictness on an INNER JOIN."""
    j = ch_join(users, orders, users.c.id == orders.c.user_id, distribution="GLOBAL")
    sql = compile_query(db.select(users.c.name).select_from(j))
    assert "GLOBAL INNER JOIN" in sql
    assert "ALL" not in sql
    assert "ANY" not in sql


def test_global_all_left_outer_join():
    j = ch_join(users, orders, users.c.id == orders.c.user_id, isouter=True, strictness="ALL", distribution="GLOBAL")
    sql = compile_query(db.select(users.c.name).select_from(j))
    assert "GLOBAL ALL LEFT OUTER JOIN" in sql


def test_global_asof_left_outer_join():
    j = ch_join(users, orders, users.c.id == orders.c.user_id, isouter=True, strictness="ASOF", distribution="GLOBAL")
    sql = compile_query(db.select(users.c.name).select_from(j))
    assert "GLOBAL ASOF LEFT OUTER JOIN" in sql


def test_no_modifiers_inner_join():
    j = ch_join(users, orders, users.c.id == orders.c.user_id)
    sql = compile_query(db.select(users.c.name).select_from(j))
    assert " INNER JOIN " in sql
    assert "ALL" not in sql
    assert "GLOBAL" not in sql


def test_standard_join_unchanged():
    j = users.join(orders, users.c.id == orders.c.user_id)
    sql = compile_query(db.select(users.c.name).select_from(j))
    assert " INNER JOIN " in sql
    assert "ALL" not in sql
    assert "GLOBAL" not in sql


def test_standard_outerjoin_unchanged():
    j = users.outerjoin(orders, users.c.id == orders.c.user_id)
    sql = compile_query(db.select(users.c.name).select_from(j))
    assert " LEFT OUTER JOIN " in sql


def test_case_insensitive_strictness():
    j = ch_join(users, orders, users.c.id == orders.c.user_id, strictness="all")
    sql = compile_query(db.select(users.c.name).select_from(j))
    assert "ALL INNER JOIN" in sql


def test_case_insensitive_distribution():
    j = ch_join(users, orders, users.c.id == orders.c.user_id, distribution="global")
    sql = compile_query(db.select(users.c.name).select_from(j))
    assert "GLOBAL INNER JOIN" in sql


def test_invalid_strictness_raises():
    with pytest.raises(ValueError, match="Invalid strictness"):
        ch_join(users, orders, users.c.id == orders.c.user_id, strictness="PARTIAL")


def test_invalid_distribution_raises():
    with pytest.raises(ValueError, match="Invalid distribution"):
        ch_join(users, orders, users.c.id == orders.c.user_id, distribution="LOCAL")


def test_cross_join_with_strictness_raises():
    with pytest.raises(ValueError, match="CROSS JOIN"):
        ch_join(users, orders, cross=True, strictness="ALL")


def test_cross_join_with_onclause_raises():
    with pytest.raises(ValueError, match="cross=True conflicts"):
        ch_join(users, orders, users.c.id == orders.c.user_id, cross=True)


def test_cross_join_with_isouter_raises():
    with pytest.raises(ValueError, match="isouter or full"):
        ch_join(users, orders, cross=True, isouter=True)


def test_cross_join_with_full_raises():
    with pytest.raises(ValueError, match="isouter or full"):
        ch_join(users, orders, cross=True, full=True)


def test_semi_inner_raises():
    with pytest.raises(ValueError, match="SEMI JOIN requires isouter=True"):
        ch_join(users, orders, users.c.id == orders.c.user_id, strictness="SEMI")


def test_anti_inner_raises():
    with pytest.raises(ValueError, match="ANTI JOIN requires isouter=True"):
        ch_join(users, orders, users.c.id == orders.c.user_id, strictness="ANTI")


def test_asof_full_join_raises():
    with pytest.raises(ValueError, match="ASOF is not supported with FULL"):
        ch_join(users, orders, users.c.id == orders.c.user_id, full=True, strictness="ASOF")


def test_cross_join():
    j = ch_join(users, orders, cross=True)
    sql = compile_query(db.select(users.c.name).select_from(j))
    assert "CROSS JOIN" in sql
    assert "ON" not in sql


def test_global_cross_join():
    j = ch_join(users, orders, cross=True, distribution="GLOBAL")
    sql = compile_query(db.select(users.c.name).select_from(j))
    assert "GLOBAL CROSS JOIN" in sql
    assert "ON" not in sql


def test_chained_joins():
    j1 = ch_join(users, orders, users.c.id == orders.c.user_id, strictness="ALL")
    j2 = ch_join(j1, items, orders.c.id == items.c.order_id, strictness="ANY")
    sql = compile_query(db.select(users.c.name, items.c.sku).select_from(j2))
    assert "ALL INNER JOIN" in sql
    assert "ANY INNER JOIN" in sql
