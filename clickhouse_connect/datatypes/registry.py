import logging

from typing import Tuple, Dict
from clickhouse_connect.datatypes.base import TypeDef, ClickHouseType, type_map
from clickhouse_connect.driver.exceptions import InternalError
from clickhouse_connect.driver.parser import parse_enum, parse_callable


def parse_name(name: str) -> Tuple[str, str, TypeDef]:
    base = name
    wrappers = []
    keys = tuple()
    if base.startswith('LowCardinality'):
        wrappers.append('LowCardinality')
        base = base[15:-1]
    if base.startswith('Nullable'):
        wrappers.append('Nullable')
        base = base[9:-1]
    if base.startswith('Enum'):
        keys, values = parse_enum(base)
        base = base[:base.find('(')]
    else:
        try:
            base, values, _ = parse_callable(base)
        except IndexError:
            raise InternalError(f'Can not parse ClickHouse data type: {name}') from None
    return base, name, TypeDef(tuple(wrappers), keys, values)


def get_from_name(name: str) -> ClickHouseType:
    ch_type = type_cache.get(name, None)
    if not ch_type:
        base, name, type_def = parse_name(name)
        try:
            ch_type = type_map[base].build(type_def)
        except KeyError:
            err_str = f'Unrecognized ClickHouse type base: {base} name: {name}'
            logging.error(err_str)
            raise InternalError(err_str) from None
        type_cache[name] = ch_type
    return ch_type


type_cache: Dict[str, ClickHouseType] = {}
