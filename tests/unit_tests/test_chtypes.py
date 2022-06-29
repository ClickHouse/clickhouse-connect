# pylint: disable=protected-access
from clickhouse_connect.datatypes.container import Nested
from clickhouse_connect.datatypes.registry import get_from_name as gfn


def test_enum_parse():
    enum_type = gfn("Enum8('OZC|8;' = -125, '6MQ4v-t' = -114, 'As7]sEg\\'' = 40, 'v~l$PR5' = 84)")
    assert 'OZC|8;' in enum_type._name_map
    enum_type = gfn('Enum8(\'\\\'"2Af\' = 93,\'KG;+\\\' = -114,\'j0\' = -40)')
    assert '\'"2Af' in enum_type._name_map
    enum_type = gfn("Enum8('value1' = 7, 'value2'=5)")
    assert enum_type.name == "Enum8('value2' = 5, 'value1' = 7)"
    assert 7 in enum_type._int_map
    assert 5 in enum_type._int_map
    enum_type = gfn(r"Enum16('beta&&' = -3, '' = 0, 'alpha\'' = 3822)")
    assert r"alpha'" == enum_type._int_map[3822]
    assert -3 == enum_type._name_map['beta&&']


def test_names():
    array_type = gfn('Array(Nullable(FixedString(50)))')
    assert array_type.name == 'Array(Nullable(FixedString(50)))'
    array_type = gfn(
        "Array(Enum8(\'user_name\' = 1, \'ip_address\' = -2, \'forwarded_ip_address\' = 3, \'client_key\' = 4))")
    assert array_type.name == (
        "Array(Enum8('ip_address' = -2, 'user_name' = 1, 'forwarded_ip_address' = 3, 'client_key' = 4))")


def test_nested_parse():
    nested_type = gfn('Nested(str1 String, int32 UInt32)')
    assert nested_type.name == 'Nested(str1 String, int32 UInt32)'
    assert isinstance(nested_type, Nested)
    nested_type = gfn('Nested(id Int64, data Nested(inner_key String, inner_map Map(String, UUID)))')
    assert nested_type.name == 'Nested(id Int64, data Nested(inner_key String, inner_map Map(String, UUID)))'
