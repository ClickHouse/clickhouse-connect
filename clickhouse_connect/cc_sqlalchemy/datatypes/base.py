import logging
from typing import Dict, Type

from sqlalchemy.exc import CompileError
from clickhouse_connect.datatypes.base import ClickHouseType, TypeDef
from clickhouse_connect.datatypes.registry import parse_name, get_from_name, get_type


class ChSqlaType:
    ch_type: [ClickHouseType] = None
    _instance = None
    _instance_cache = None

    def __init_subclass__(cls, **kwargs):
        sqla_type_map[cls.__name__.upper()] = cls
        if '__init__' in cls.__dict__:
            cls._instance_cache: Dict[TypeDef, 'ChSqlaType'] = {}
        else:
            cls.ch_type = get_from_name(cls.__name__)
            cls._instance = cls()

    @classmethod
    def build(cls, base: str, type_def: TypeDef):
        if cls._instance:
            return cls._instance
        instance = cls._instance_cache.get(type_def)
        if instance:
            return instance
        ch_type = get_type(base).build(type_def)
        kwargs = cls._init_args(type_def)
        # noinspection PyArgumentList
        instance = cls(**kwargs)
        instance.ch_type = ch_type
        cls._instance_cache[type_def] = instance
        return instance

    @classmethod
    def _init_args(cls, _type_def: TypeDef):
        return {}

    @staticmethod
    def result_processor():  # The driver handles type conversions to python datatypes
        return None

    def _compiler_dispatch(self, _visitor, **_): # The driver handles name conversions
        return self.ch_type.name


sqla_type_map: Dict[str, Type[ChSqlaType]] = {}


def sqla_type_from_name(name: str) -> ChSqlaType:
    base, name, type_def = parse_name(name)
    try:
        type_cls = sqla_type_map[base]
    except KeyError:
        err_str = f'Unrecognized ClickHouse type base: {base} name: {name}'
        logging.error(err_str)
        raise CompileError(err_str)
    return type_cls.build(base, type_def)