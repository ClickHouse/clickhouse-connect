import array
import decimal
import sys
from typing import Union

must_swap = sys.byteorder == 'big'
int_size = array.array('i').itemsize


array_map = {1: 'b', 2: 'h', 4: 'i', 8: 'q'}

if int_size == 2:
    array_map[4] = 'l'


def array_type(size: int, signed:bool = True):
    try:
        at = array_map[size]
    except KeyError:
        return None
    return at if signed else at.upper()


def read_leb128(source: Union[bytes, memoryview, bytearray], loc: int):
    length = 0
    ix = 0
    while True:
        b = source[loc + ix]
        length = length + ((b & 0x7f) << (ix * 7))
        ix += 1
        if (b & 0x80) == 0:
            break
    return length, loc + ix


def read_leb128_str(source: Union[memoryview, bytes, bytearray], loc: int, encoding: str = 'utf8'):
    length, loc = read_leb128(source, loc)
    return str(source[loc:loc + length], encoding), loc + length


def to_leb128(value: int) -> bytearray:  #Unsigned only
    result = bytearray()
    while True:
        b = value & 0x7f
        value = value >> 7
        if value == 0:
            result.append(b)
            break
        result.append(0x80 | b)
    return result

