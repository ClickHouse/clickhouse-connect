import pytest
from sqlalchemy import Column, MetaData, Table, column, func, select
from sqlalchemy.dialects import registry

# Import sql module so any monkey-patches are installed.
import clickhouse_connect.cc_sqlalchemy.sql  # noqa: F401
from clickhouse_connect.cc_sqlalchemy import Lambda, dialect_name
from clickhouse_connect.cc_sqlalchemy.datatypes.sqltypes import Array, String, UInt32

dialect = registry.load(dialect_name)()
metadata = MetaData()

data = Table(
    "data",
    metadata,
    Column("id", UInt32),
    Column("nums", Array(UInt32)),
    Column("ks", Array(String)),
    Column("vs", Array(UInt32)),
)


def compile_sql(stmt):
    return str(stmt.compile(dialect=dialect, compile_kwargs={"literal_binds": True}))


def compile_default(stmt):
    """Compile with SA's default StrSQLCompiler (no dialect)."""
    return str(stmt.compile(compile_kwargs={"literal_binds": True}))


def _strip_backticks(sql):
    return sql.replace("`", "")


def test_lambda_array_map_single_param():
    stmt = select(func.arrayMap(Lambda("x", column("x") * 2), data.c.nums).label("doubled"))
    sql = compile_sql(stmt)
    assert "arrayMap(" in sql
    assert "x -> " in sql
    assert "x -> x * 2" in _strip_backticks(sql)


def test_lambda_array_filter_comparison_body():
    stmt = select(func.arrayFilter(Lambda("x", column("x") > 0), data.c.nums).label("positives"))
    sql = compile_sql(stmt)
    assert "arrayFilter(" in sql
    assert "x -> " in sql
    assert "x -> x > 0" in _strip_backticks(sql)


def test_lambda_multi_param():
    stmt = select(func.arrayMap(Lambda(["k", "v"], column("v") > 10), data.c.ks, data.c.vs).label("pairs"))
    sql = compile_sql(stmt)
    assert "arrayMap(" in sql
    assert "(k, v) -> " in sql
    assert "(k, v) -> v > 10" in _strip_backticks(sql)


def test_lambda_body_is_func_call():
    stmt = select(func.arrayMap(Lambda("x", func.toString(column("x"))), data.c.nums).label("strs"))
    sql = compile_sql(stmt)
    assert "x -> " in sql
    assert "x -> toString(x)" in _strip_backticks(sql)


def test_lambda_renders_under_both_compilers():
    """Lambda renders identically under the dialect compiler and the default StrSQLCompiler."""
    stmt = select(func.arrayMap(Lambda("x", column("x") * 2), data.c.nums).label("doubled"))
    dialect_sql = compile_sql(stmt)
    default_sql = compile_default(stmt)
    assert "x -> x * 2" in _strip_backticks(dialect_sql)
    assert "x -> x * 2" in _strip_backticks(default_sql)


def test_lambda_empty_params_raises_value_error():
    with pytest.raises(ValueError):
        Lambda([], column("x"))


def test_lambda_non_string_param_in_list_raises_type_error():
    with pytest.raises(TypeError):
        Lambda([1, 2], column("x"))


def test_lambda_non_string_single_param_raises_type_error():
    with pytest.raises(TypeError):
        Lambda(1, column("x"))


def test_lambda_non_identifier_param_raises_value_error():
    with pytest.raises(ValueError):
        Lambda("x y", column("x"))
