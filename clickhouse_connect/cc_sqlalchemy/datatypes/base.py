import logging
from typing import Dict, Type

from sqlalchemy.exc import CompileError
from clickhouse_connect.datatypes.base import ClickHouseType, TypeDef, NULL_TYPE_DEF
from clickhouse_connect.datatypes.registry import parse_name, get_type


class ChSqlaType:
    ch_type: ClickHouseType = None
    generic_type: None
    _ch_type_cls = None
    _instance = None
    _instance_cache: Dict[TypeDef, 'ChSqlaType'] = None

    def __init_subclass__(cls, registered: bool = True, **kwargs):
        if not registered:
            return
        base = cls.__name__.upper()
        schema_types.append(cls.__name__)
        sqla_type_map[base] = cls
        if not cls._ch_type_cls:
            cls._ch_type_cls = get_type(base, cls.__name__)
        cls._instance_cache = {}

    @classmethod
    def build(cls, type_def: TypeDef):
        return cls._instance_cache.setdefault(type_def, cls(type_def = type_def))

    def __init__(self, type_def: TypeDef = NULL_TYPE_DEF):
        self.ch_type = self._ch_type_cls.build(type_def)

    @property
    def nullable(self):
        return self.ch_type.nullable

    @staticmethod
    def result_processor():  # The driver handles type conversions to python datatypes
        return None

    @staticmethod
    def _cached_result_processor(*_):
        return None

    def _compiler_dispatch(self, _visitor, **_):  # The driver handles name conversions
        return self.ch_type.name


sqla_type_map: Dict[str, Type[ChSqlaType]] = {}
schema_types = []


def sqla_type_from_name(name: str) -> ChSqlaType:
    base, name, type_def = parse_name(name)
    try:
        type_cls = sqla_type_map[base]
    except KeyError:
        err_str = f'Unrecognized ClickHouse type base: {base} name: {name}'
        logging.error(err_str)
        raise CompileError(err_str)
    return type_cls.build(type_def)
