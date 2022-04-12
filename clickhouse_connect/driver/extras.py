from random import random
from typing import Union

from clickhouse_connect.datatypes.base import ClickHouseType
from clickhouse_connect.datatypes.container import Array
from clickhouse_connect.datatypes.numeric import BigInt
from clickhouse_connect.datatypes.registry import get_from_name
from clickhouse_connect.datatypes.string import String, FixedString
from clickhouse_connect.driver.common import array_sizes


def random_col_data(ch_type: Union[str, ClickHouseType], cnt: int = 1,
                    str_len: int = 32, arr_len: int = 8, ascii_only=True):
    return _random_col_data(ch_type, cnt, str_len, arr_len, ascii_only)


# pylint: disable=protected-access
def _random_col_data(ch_type: Union[str, ClickHouseType], cnt: int = 1,
                    str_len: int = 32, arr_len: int = 8, ascii_only=True):
    if isinstance(ch_type, str):
        ch_type = get_from_name(ch_type)
    if isinstance(ch_type, BigInt) or ch_type.python_type == int:
        if isinstance(ch_type, BigInt):
            sz = ch_type._byte_size
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
        gen = lambda: list(random_col_data(ch_type.element_type, arr_len, str_len, arr_len))
    elif isinstance(ch_type, String):
        char_max = 127 - 32 if ascii_only else 32767 - 32
        gen = lambda: ''.join((chr(int(random() * char_max) + 32) for _ in range(int(random() * str_len))))
    elif isinstance(ch_type, FixedString):
        gen = lambda: bytes((int(random() * 256) for _ in range(ch_type._byte_size)))
    else:
        raise ValueError(f'Invalid ClickHouse type {ch_type.name} for random column data')
    return tuple(gen() for _ in range(cnt))
