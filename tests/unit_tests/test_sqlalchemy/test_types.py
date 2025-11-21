from sqlalchemy import Integer, DateTime

from clickhouse_connect.cc_sqlalchemy.datatypes.sqltypes import Nullable, Int64, DateTime64, LowCardinality, String, QBit
from clickhouse_connect.cc_sqlalchemy.datatypes.base import sqla_type_map, sqla_type_from_name


def test_mapping():
    assert issubclass(sqla_type_map['UInt64'], Integer)
    assert issubclass(sqla_type_map['DateTime'], DateTime)


# pylint: disable=protected-access
def test_sqla():
    int16 = sqla_type_from_name('Int16')
    assert 'Int16' == int16._compiler_dispatch(None)
    enum = sqla_type_from_name("Enum8('value1' = 7, 'value2'=5)")
    assert "Enum8('value2' = 5, 'value1' = 7)" == enum._compiler_dispatch(None)


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


def test_qbit():
    qbit = sqla_type_from_name('QBit(Float32, 768)')
    assert qbit.__class__ == QBit
    assert qbit.name == 'QBit(Float32, 768)'
    assert qbit._compiler_dispatch(None) == 'QBit(Float32, 768)'

    qbit2 = QBit('Float32', 768)
    assert qbit2.name == 'QBit(Float32, 768)'

    qbit_bf16 = sqla_type_from_name('QBit(BFloat16, 128)')
    assert qbit_bf16.name == 'QBit(BFloat16, 128)'

    qbit_f64 = sqla_type_from_name('QBit(Float64, 1536)')
    assert qbit_f64.name == 'QBit(Float64, 1536)'
