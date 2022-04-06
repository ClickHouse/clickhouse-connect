from sqlalchemy import Integer, DateTime

from clickhouse_connect.cc_sqlalchemy.datatypes.sqltypes import Nullable, Int64, DateTime64, LowCardinality, String
from clickhouse_connect.cc_sqlalchemy.datatypes.base import sqla_type_map, sqla_type_from_name


def test_mapping():
    assert issubclass(sqla_type_map['UINT16'], Integer)
    assert issubclass(sqla_type_map['DATETIME'], DateTime)


# pylint: disable=protected-access
def test_sqla():
    int16 = sqla_type_from_name('Int16')
    assert 'Int16' == int16._compiler_dispatch(None)
    enum = sqla_type_from_name("Enum8('value1' = 7, 'value2'=5)")
    assert "Enum8('value1' = 7, 'value2' = 5)" == enum._compiler_dispatch(None)


# pylint: disable=no-member
def test_nullable():
    nullable = Nullable(Int64)
    assert nullable.__class__ == Int64
    nullable = Nullable(DateTime64(6))
    assert nullable.__class__ == DateTime64
    assert nullable.name == 'Nullable(DateTime64(6))'


# pylint: disable=no-member
def test_low_cardinality():
    lc_str = LowCardinality(Nullable(String))
    assert lc_str.__class__ == String
    assert lc_str.name == 'LowCardinality(Nullable(String))'
