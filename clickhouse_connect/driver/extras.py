import array
import uuid
from random import random
from typing import Union, NamedTuple

from clickhouse_connect.datatypes.base import ClickHouseType
from clickhouse_connect.datatypes.container import Array
from clickhouse_connect.datatypes.numeric import BigInt, Float32, Float64
from clickhouse_connect.datatypes.registry import get_from_name
from clickhouse_connect.datatypes.special import UUID
from clickhouse_connect.datatypes.string import String, FixedString
from clickhouse_connect.driver.common import array_sizes


class RandomColDef(NamedTuple):
    null_pct: float = 0.15
    str_len: int = 200
    arr_len: int = 8
    ascii_only: bool = True


def random_float():
    return (random() * random() * 65536) / (random() * (random() * 256 - 128))


def random_col_data(ch_type: Union[str, ClickHouseType], cnt: int, col_def: RandomColDef = RandomColDef()):
    return _random_col_data(ch_type,cnt,  col_def)


# pylint: disable=protected-access,too-many-branches
def _random_col_data(ch_type: Union[str, ClickHouseType], cnt: int, col_def:RandomColDef):
    if isinstance(ch_type, str):
        ch_type = get_from_name(ch_type)
    if isinstance(ch_type, BigInt) or ch_type.python_type == int:
        if isinstance(ch_type, BigInt):
            sz = 2 ** (ch_type._byte_size * 8)
            signed = ch_type._signed
        else:
            sz = 2 ** (array_sizes[ch_type._array_type.lower()] * 8)
            signed = ch_type._array_type == ch_type._array_type.lower()
        if signed:
            sub = sz >> 1
            gen = lambda: int(random() * sz) - sub
        else:
            gen = lambda: int(random() * sz)
    elif isinstance(ch_type, Array):
        gen = lambda: list(random_col_data(ch_type.element_type, int(random() * col_def.arr_len), col_def))
    elif isinstance(ch_type, String):
        char_max = 127 - 32 if col_def.ascii_only else 32767 - 32
        gen = lambda: ''.join((chr(int(random() * char_max) + 32) for _ in range(int(random() * col_def.str_len))))
    elif isinstance(ch_type, FixedString):
        gen = lambda: bytes((int(random() * 256) for _ in range(ch_type._byte_size)))
    elif isinstance(ch_type, Float64):
        gen = random_float
    elif isinstance(ch_type, Float32):
        return tuple(array.array('f', [random_float() for _ in range(cnt)]))
    elif isinstance(ch_type, UUID):
        gen = uuid.uuid4
    else:
        raise ValueError(f'Invalid ClickHouse type {ch_type.name} for random column data')
    if ch_type.nullable:
        x = col_def.null_pct
        return tuple(gen() if random() > x else None for _ in range(cnt))
    return tuple(gen() for _ in range(cnt))
