from unittest import TestCase

from sqlalchemy import Integer

from clickhouse_connect.chtypes.registry import get_from_name
from clickhouse_connect.sqlalchemy import map_schema_types


class TestSchemaTypes(TestCase):

    def test_mapping(self):
        schema_mapping = map_schema_types()
        assert(issubclass(schema_mapping['UInt32'], Integer))

    def test_sqla(self):
        int16 = get_from_name('Int16')
        sqla_type = int16.get_sqla_type()
        assert (sqla_type.compile() == 'Int16')
        enum = get_from_name("Enum8('value1' = 7, 'value2'=5)")
        sqla_type = enum.get_sqla_type()
        assert (sqla_type.compile() == "Enum8('value1' = 7, 'value2' = 5)")