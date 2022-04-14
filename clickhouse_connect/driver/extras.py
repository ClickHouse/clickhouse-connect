import struct
import uuid
from collections.abc import Sequence
from random import random
from typing import Union, NamedTuple

from clickhouse_connect.datatypes.base import ClickHouseType
from clickhouse_connect.datatypes.container import Array, Tuple
from clickhouse_connect.datatypes.numeric import BigInt, Float32, Float64
from clickhouse_connect.datatypes.registry import get_from_name
from clickhouse_connect.datatypes.special import UUID
from clickhouse_connect.datatypes.string import String, FixedString
from clickhouse_connect.driver.common import array_sizes


class RandomValueDef(NamedTuple):
    null_pct: float = 0.15
    str_len: int = 200
    arr_len: int = 8
    ascii_only: bool = True


def random_float():
    return (random() * random() * 65536) / (random() * (random() * 256 - 128))


def random_float32():
    f64 = (random() * random() * 65536) / (random() * (random() * 256 - 128))
    return struct.unpack('f', struct.pack('f', f64))[0]


def random_tuple(element_types: Sequence[ClickHouseType], col_def):
    return tuple(random_value_gen(x, col_def)() for x in element_types)


def random_col_data(ch_type: Union[str, ClickHouseType], cnt: int, col_def: RandomValueDef = RandomValueDef()):
    if isinstance(ch_type, str):
        ch_type = get_from_name(ch_type)
    gen = random_value_gen(ch_type, col_def)
    if ch_type.nullable:
        x = col_def.null_pct
        return tuple(gen() if random() > x else None for _ in range(cnt))
    return tuple(gen() for _ in range(cnt))


# pylint: disable=too-many-return-statements,protected-access
def random_value_gen(ch_type: ClickHouseType,  col_def:RandomValueDef):
    if isinstance(ch_type, BigInt) or ch_type.python_type == int:
        if isinstance(ch_type, BigInt):
            sz = 2 ** (ch_type._byte_size * 8)
            signed = ch_type._signed
        else:
            sz = 2 ** (array_sizes[ch_type._array_type.lower()] * 8)
            signed = ch_type._array_type == ch_type._array_type.lower()
        if signed:
            sub = sz >> 1
            return lambda: int(random() * sz) - sub
        return lambda: int(random() * sz)
    if isinstance(ch_type, Array):
        return lambda: list(random_col_data(ch_type.element_type, int(random() * col_def.arr_len), col_def))
    if isinstance(ch_type, Tuple):
        return lambda: random_tuple(ch_type.element_types, col_def)
    if isinstance(ch_type, String):
        char_max = 127 - 32 if col_def.ascii_only else 32767 - 32
        return lambda: ''.join((chr(int(random() * char_max) + 32) for _ in range(int(random() * col_def.str_len))))
    if isinstance(ch_type, FixedString):
        return lambda: bytes((int(random() * 256) for _ in range(ch_type._byte_size)))
    if isinstance(ch_type, Float64):
        return random_float
    if isinstance(ch_type, Float32):
        return random_float32
    if isinstance(ch_type, UUID):
        return uuid.uuid4
    raise ValueError(f'Invalid ClickHouse type {ch_type.name} for random column data')
