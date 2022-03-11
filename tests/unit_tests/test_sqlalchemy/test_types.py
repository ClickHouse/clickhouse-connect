

from sqlalchemy import Integer, DateTime

from clickhouse_connect.datatypes.registry import get_from_name
from clickhouse_connect.sqlalchemy.datatypes import map_schema_types


def test_mapping():
    schema_mapping = map_schema_types()
    assert issubclass(schema_mapping['UINT16'], Integer)
    assert issubclass(schema_mapping['DATETIME64'], DateTime)


def test_sqla():
    int16 = get_from_name('Int16')
    sqla_type = int16.sqla_type
    assert 'Int16' == sqla_type.compile()
    enum = get_from_name("Enum8('value1' = 7, 'value2'=5)")
    sqla_type = enum.sqla_type
    assert "Enum8('value1' = 7, 'value2' = 5)" == sqla_type.compile()
