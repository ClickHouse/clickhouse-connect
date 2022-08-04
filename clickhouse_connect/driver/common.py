import array
import sys

from typing import Tuple, Sequence, MutableSequence

# pylint: disable=invalid-name
must_swap = sys.byteorder == 'big'
int_size = array.array('i').itemsize
low_card_version = 1

array_map = {1: 'b', 2: 'h', 4: 'i', 8: 'q'}
decimal_prec = {32: 9, 64: 18, 128: 38, 256: 79}

if int_size == 2:
    array_map[4] = 'l'

array_sizes = {v: k for k, v in array_map.items()}
array_sizes['f'] = 4
array_sizes['d'] = 8


def array_type(size: int, signed: bool):
    """
    Determines the Python array.array code for the requested byte size
    :param size: byte size
    :param signed: whether int types should be signed or unsigned
    :return: Python array.array code
    """
    try:
        code = array_map[size]
    except KeyError:
        return None
    return code if signed else code.upper()


def array_column(code: str, source: Sequence, loc: int, num_rows: int) -> Tuple[array.array, int]:
    """
    Read the source binary buffer into a Python array.array
    :param code: Python array.array type code
    :param source: Source byte buffer like object
    :param loc: Start read position in the buffer
    :param num_rows: Number of rows to read
    :return: array.array of the requested data type plus next buffer read location
    """
    column = array.array(code)
    sz = column.itemsize * num_rows
    end = loc + sz
    column.frombytes(source[loc: end])
    if must_swap:
        column.byteswap()
    return column, end


def write_array(code: str, column: Sequence, dest: MutableSequence):
    """
    Write a column of native Python data matching the array.array code
    :param code: Python array.array code matching the column data type
    :param column: Column of native Python values
    :param dest: Destination byte buffer
    """
    if column and not isinstance(column[0], (int, float)):
        if code in ('f', 'F', 'd', 'D'):
            column = [float(x) for x in column]
        else:
            column = [int(x) for x in column]
    buff = array.array(code, column)
    if must_swap:
        buff.byteswap()
    dest += buff.tobytes()


def read_uint64(source: Sequence, loc: int) -> Tuple[int, int]:
    """
    Read a single UInt64 value from a data buffer
    :param source: Source binary buffer
    :param loc: Source start location
    :return: UInt64 value from buffer plus new source location
    """
    return int.from_bytes(source[loc: loc + 8], 'little'), loc + 8


def write_uint64(value: int, dest: MutableSequence):
    """
    Write a single UInt64 value to a binary write buffer
    :param value: UInt64 value to write
    :param dest: Destination byte buffer
    """
    dest.extend(value.to_bytes(8, 'little'))


def read_leb128(source: Sequence, loc: int) -> Tuple[int, int]:
    """
    Read a LEB128 encoded integer value from a source buffer
    :param source: Source binary buffer
    :param loc: Start location
    :return: Leb128 value plus next read location
    """
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
    """
    Read a LEB128 encoded string from a binary source buffer
    :param source: Binary source buffer
    :param loc: read start location
    :param encoding: expected string encoding
    :return: Complete string plus new read location in source buffer
    """
    length, loc = read_leb128(source, loc)
    return str(source[loc:loc + length], encoding), loc + length


def write_leb128(value: int, dest: MutableSequence):
    """
    Write a LEB128 encoded integer to a target binary buffer
    :param value: Integer value (positive only)
    :param dest: Target buffer
    """
    while True:
        b = value & 0x7f
        value >>= 7
        if value == 0:
            dest.append(b)
            return
        dest.append(0x80 | b)


def to_leb128(value: int) -> bytearray:
    """
    Create a byte array representing a LEB128 encoded integer
    :param value: Integer value to encode
    :return: bytearray with encoding value
    """
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
    """
    Determine the bit size of a ClickHouse or Python Decimal needed to store a value of the requested precision
    :param prec: Precision of the Decimal in total number of base 10 digits
    :return: Required bit size
    """
    if prec < 1 or prec > 79:
        raise ArithmeticError(f'Invalid precision {prec} for ClickHouse Decimal type')
    if prec < 10:
        return 32
    if prec < 19:
        return 64
    if prec < 39:
        return 128
    return 256


def unescape_identifier(x: str) -> str:
    if x.startswith('`') and x.endswith('`'):
        return x[1:-1]
    return x
