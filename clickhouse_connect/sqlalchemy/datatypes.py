import re
from typing import Type

from sqlalchemy import Integer, Float, String, DateTime, Date, Boolean, LargeBinary
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.sql.type_api import TypeEngine

from clickhouse_connect.datatypes.datatypes import ClickHouseType
from clickhouse_connect.datatypes.registry import type_map


def sqla_compile(self, *args, **kwargs):
    return self.ch_type.name


def get_sqla_type(self):
    sqla_type = self.sqla_type_cls()
    sqla_type.ch_type = self
    return sqla_type


def ch_to_sqla_type(ch_type_cls: Type[ClickHouseType], sqla_type: TypeEngine):
    sqla_type_cls = type(ch_type_cls.__name__.upper(), (sqla_type,), {})
    sqla_type_cls.compile = sqla_compile
    ch_type_cls.sqla_type_cls = sqla_type_cls
    ch_type_cls.get_sqla_type = get_sqla_type
    return sqla_type_cls


type_mapping = (
    (r'^U?INT(\d)*$', Integer),
    (r'^FLOAT\d*$', Float),
    (r'^ENUM', String),
    (r'(FIXED)?STRING', String),
    (r'^UUID', LargeBinary),
    (r'^ARRAY', TypeEngine),
    (r'^TUPLE', TypeEngine),
    (r'^DATETIME$', DateTime),
    (r'^DATE$', Date),
    (r'^BOOL', Boolean),
)


def map_schema_types():
    compiled = [(re.compile(pattern), sqla_base) for pattern, sqla_base in type_mapping]
    schema_types = {}
    for name, ch_type_cls in type_map.items():
        found = False
        for pattern, sqla_base in compiled:
            match = pattern.match(name)
            if match:
                schema_types[name] = ch_to_sqla_type(ch_type_cls, sqla_base)
                found = True
                break
        if not found:
            raise SQLAlchemyError(f"Unmapped ClickHouse type {name}")
    return schema_types





