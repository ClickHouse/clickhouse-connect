import decimal
import clickhouse_connect.datatypes
from clickhouse_connect.datatypes.registry import get_from_name as gfn


def test_enum_parse():
    enum_type = gfn("Enum8('value1' = 7, 'value2'=5)")
    assert enum_type.name == "Enum8('value1' = 7, 'value2' = 5)"
    assert 7 in enum_type._int_map
    assert 5 in enum_type._int_map
    enum_type = gfn(r"Enum16('beta&&' = -3, '' = 0, 'alpha\'' = 3822)")
    assert r"alpha'" == enum_type._int_map[3822]
    assert -3 == enum_type._name_map['beta&&']


def test_names():
    array_type = gfn('Array(Nullable(FixedString(50)))')
    assert array_type.name == 'Array(Nullable(FixedString(50)))'


def test_decimal():
    dec_type = gfn('Decimal128(5)')
    source = bytearray([] * 128)
    dc = dec_type.from_row_binary(source, 0)
    assert dc ==  decimal.Decimal(3.5)



