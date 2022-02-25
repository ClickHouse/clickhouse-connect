from unittest import TestCase

from sqlalchemy import Integer

from clickhouse_connect.datatypes.registry import get_from_name
from clickhouse_connect.sqlalchemy.datatypes import map_schema_types


class TestSchemaTypes(TestCase):

    def test_mapping(self):
        schema_mapping = map_schema_types()
        assert(issubclass(schema_mapping['UINT'], Integer))

    def test_sqla(self):
        int16 = get_from_name('Int16')
        sqla_type = int16.get_sqla_type()
        assert ('Int16', sqla_type.compile())
        enum = get_from_name("Enum8('value1' = 7, 'value2'=5)")
        sqla_type = enum.get_sqla_type()
        assert ( "Enum8('value1' = 7, 'value2' = 5)", sqla_type.compile())