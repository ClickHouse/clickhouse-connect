import re
from typing import Type

from clickhouse_driver.dbapi import ProgrammingError
from sqlalchemy import Integer, Float, String, DateTime, Date
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.sql.type_api import TypeEngine

from clickhouse_connect.chtypes.datatypes import ClickHouseType
from clickhouse_connect.chtypes.registry import type_map


def sqla_compile(self, *args, **kwargs):
    return self.ch_type.label()


def get_sqla_type(self):
    sqla_type = self.sqla_type_cls()
    sqla_type.ch_type = self
    return sqla_type


def ch_to_sqla_type(ch_type_cls: Type[ClickHouseType], sqla_type: TypeEngine):
    sqla_type_cls = type(ch_type_cls.name, (sqla_type,), {})
    sqla_type_cls.compile = sqla_compile
    ch_type_cls.sqla_type_cls = sqla_type_cls
    ch_type_cls.get_sqla_type = get_sqla_type
    return sqla_type_cls


type_mapping = (
    (r'[u]?int[\d]*$', Integer),
    (r'float[\d]*$', Float),
    (r'^enum', String),
    (r'^string$', String),
    (r'^array', TypeEngine),
    (r'^datetime$', DateTime),
    (r'^date$', Date)
)


def map_schema_types():
    compiled = [(re.compile(pattern, re.IGNORECASE), sqla_base) for pattern, sqla_base in type_mapping]
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





