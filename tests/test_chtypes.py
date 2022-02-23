from unittest import TestCase
from click_alchemy.chtypes.registry import get, _parse_name as pn


class ClickHouseTypeTest(TestCase):

    def test_type_parse(self):
        enum_type = pn("Enum8('value1' = 7, 'value2'=5)")
        assert(enum_type.keys == ('value1', 'value2'))
        assert(enum_type.values == (7, 5))
        enum_type = pn(r"Enum16('beta&&' = -3, '' = 0, 'alpha\'' = 3822)")
        assert(enum_type.keys == ('beta&&', '', "alpha'"))
        assert(enum_type.values == (-3, 0, 3822))

    def test_sqla(self):
        int16 = get('Int16')
        sqla_type = int16.get_sqla_type()
        assert(sqla_type.compile() == 'Int16')
        enum = get("Enum8('value1' = 7, 'value2'=5)")
        sqla_type = enum.get_sqla_type()
        assert(sqla_type.compile() == "Enum8('value1' = 7, 'value2' = 5)")
