import array
import sys

from typing import Tuple, Sequence, MutableSequence

# pylint: disable=invalid-name
must_swap = sys.byteorder == 'big'
int_size = array.array('i').itemsize
low_card_version = 1

array_map = {1: 'b', 2: 'h', 4: 'i', 8: 'q'}

if int_size == 2:
    array_map[4] = 'l'

array_sizes = {v: k for k, v in array_map.items()}
array_sizes['f'] = 4
array_sizes['d'] = 8


def array_type(size: int, signed: bool):
    try:
        code = array_map[size]
    except KeyError:
        return None
    return code if signed else code.upper()


def array_column(code: str, source: Sequence, loc: int, num_rows: int):
    column = array.array(code)
    sz = column.itemsize * num_rows
    end = loc + sz
    column.frombytes(source[loc: end])
    if must_swap:
        column.byteswap()
    return column, end


def write_array(code: str, column: Sequence, dest: MutableSequence):
    buff = array.array(code, column)
    if must_swap:
        buff.byteswap()
    dest += buff.tobytes()


def read_uint64(source: Sequence, loc: int):
    return int.from_bytes(source[loc: loc + 8], 'little'), loc + 8


def write_uint64(value: int, dest: MutableSequence):
    dest.extend(value.to_bytes(8, 'little'))


def read_leb128(source: Sequence, loc: int):
    length = 0
    ix = 0
    while True:
        b = source[loc + ix]
        length = length + ((b & 0x7f) << (ix * 7))
        ix += 1
        if (b & 0x80) == 0:
            break
    return length, loc + ix


def read_leb128_str(source: Sequence, loc: int, encoding: str = 'utf8') -> Tuple[str, int]:
    length, loc = read_leb128(source, loc)
    return str(source[loc:loc + length], encoding), loc + length


def write_leb128(value: int, dest: MutableSequence):
    while True:
        b = value & 0x7f
        value >>= 7
        if value == 0:
            dest.append(b)
            return
        dest.append(0x80 | b)


def to_leb128(value: int) -> bytearray:  # Unsigned only
    result = bytearray()
    while True:
        b = value & 0x7f
        value >>= 7
        if value == 0:
            result.append(b)
            break
        result.append(0x80 | b)
    return result


def decimal_size(prec: int):
    if prec < 1 or prec > 79:
        raise ArithmeticError(f'Invalid precision {prec} for ClickHouse Decimal type')
    if prec < 10:
        return 32
    if prec < 19:
        return 64
    if prec < 39:
        return 128
    return 256


decimal_prec = {32: 9, 64: 18, 128: 38, 256: 79}
