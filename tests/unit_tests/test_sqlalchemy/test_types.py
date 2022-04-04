from sqlalchemy import Integer, DateTime


from clickhouse_connect.cc_sqlalchemy.datatypes.base import sqla_type_map, sqla_type_from_name


def test_mapping():
    assert issubclass(sqla_type_map['UINT16'], Integer)
    assert issubclass(sqla_type_map['DATETIME'], DateTime)


def test_sqla():
    int16 = sqla_type_from_name('Int16')
    assert 'Int16' == int16._compiler_dispatch(None)
    enum = sqla_type_from_name("Enum8('value1' = 7, 'value2'=5)")
    assert "Enum8('value1' = 7, 'value2' = 5)" == enum._compiler_dispatch(None)
