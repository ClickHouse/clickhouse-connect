from unittest import TestCase
from clickhouse_connect.chtypes.registry import get_from_name, _parse_name as pn


class ClickHouseTypeTest(TestCase):

    def test_def_parse(self):
        enum_type = pn("Enum8('value1' = 7, 'value2'=5)")
        assert(enum_type.keys == ('value1', 'value2'))
        assert(enum_type.values == (7, 5))
        enum_type = pn(r"Enum16('beta&&' = -3, '' = 0, 'alpha\'' = 3822)")
        assert(enum_type.keys == ('beta&&', '', "alpha'"))
        assert(enum_type.values == (-3, 0, 3822))

    def test_def_equality(self):
        array1 = pn('Array(LowCardinality(String))')
        array2 = pn('Array(LowCardinality(String))')
        assert(array1 == array2)
        array1 = pn('Nullable(UInt32)')
        array2 = pn('UInt32')
        assert (array1 != array2)


