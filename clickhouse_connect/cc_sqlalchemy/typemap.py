import re
from typing import Type


from sqlalchemy import Integer, Float, String, DateTime, Date, Boolean, DECIMAL
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.sql.type_api import TypeEngine, UserDefinedType

from clickhouse_connect.datatypes.base import ClickHouseType, type_map


def sqla_compile(self, **_):
    return self.ch_type.name


def get_sqla_type(self):
    try:
        return getattr(self, '_sqla_type')
    except AttributeError:
        sqla_type = self.sqla_type_cls()
        sqla_type.ch_type = self
        setattr(self, '_sqla_type', sqla_type)
        return sqla_type


def ch_to_sqla_type(ch_type_cls: Type[ClickHouseType], sqla_type_engine: TypeEngine):
    sqla_type_cls = type(ch_type_cls.__name__.upper(), (sqla_type_engine,), {})
    sqla_type_cls.compile = sqla_compile
    ch_type_cls.sqla_type_cls = sqla_type_cls
    ch_type_cls.sqla_type = property(get_sqla_type)
    return sqla_type_cls


type_mapping = (
    (r'^U?INT(\d)*$', Integer),
    (r'^FLOAT\d*$', Float),
    (r'^ENUM', String),
    (r'(FIXED)?STRING', String),
    (r'^(NOTHING|UUID|ARRAY|TUPLE|MAP|IP|DECIMAL|OBJECT|NESTED|JSON)', UserDefinedType),
    (r'(SIMPLE)?AGGREGATEFUNCTION$', UserDefinedType),
    (r'^DATETIME', DateTime),
    (r'^DATE', Date),
    (r'^BOOL', Boolean),
)


def map_schema_types():
    compiled = [(re.compile(pattern), sqla_base) for pattern, sqla_base in type_mapping]
    schema_types = {}
    for name, ch_type_cls in type_map.items():
        for pattern, sqla_base in compiled:
            match = pattern.match(name)
            if match:
                schema_types[name] = ch_to_sqla_type(ch_type_cls, sqla_base)
                break
        else:
            raise SQLAlchemyError(f"Unmapped ClickHouse type {name}")
    return schema_types





