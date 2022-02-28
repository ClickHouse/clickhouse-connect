from unittest import TestCase
from clickhouse_connect.datatypes.registry import get_from_name as gfn


class ClickHouseTypeTest(TestCase):

    def test_enum_parse(self):
        enum_type = gfn("Enum8('value1' = 7, 'value2'=5)")
        assert enum_type.name == "Enum8('value1' = 7, 'value2' = 5)"
        assert 7 in enum_type._int_map
        assert 5 in enum_type._int_map
        enum_type = gfn(r"Enum16('beta&&' = -3, '' = 0, 'alpha\'' = 3822)")
        assert 2 == enum_type.size
        assert r"alpha'" == enum_type._int_map[3822]
        assert -3 == enum_type._name_map['beta&&']

    def test_names(self):
        array_type = gfn('Array(Nullable(FixedString(50)))')
        assert array_type.name == 'Array(Nullable(FixedString(50)))'



