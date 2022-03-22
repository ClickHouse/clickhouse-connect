import decimal
from collections.abc import Sequence
from functools import partial

from typing import Any, Union, Iterable
from struct import unpack_from as suf, pack as sp

from clickhouse_connect.datatypes.base import TypeDef, FixedType
from clickhouse_connect.datatypes.tools import array_type


class Int8(FixedType):
    _array_type = 'b'

    @staticmethod
    def _from_row_binary(source: bytes, loc: int):
        x = source[loc]
        return x if x < 128 else x - 128, loc + 1

    @staticmethod
    def _to_row_binary(value: int, dest: bytearray):
        if value < 128:
            dest.append(value)
        else:
            dest.append(value + 128)


class UInt8(FixedType):
    _array_type = 'B'

    @staticmethod
    def _from_row_binary(source: bytes, loc: int):
        return source[loc], loc + 1

    @staticmethod
    def _to_row_binary(value:int, dest: bytearray):
        dest.append(value)


class Int16(FixedType):
    _array_type = 'h'

    @staticmethod
    def _from_row_binary(source: bytes, loc:int):
        return suf('<h', source, loc)[0], loc + 2

    @staticmethod
    def _to_row_binary(value: int, dest: bytearray):
        dest += sp('<h', value,)


class UInt16(FixedType):
    _array_type = 'H'

    @staticmethod
    def _from_row_binary(source: bytes, loc: int):
        return suf('<H', source, loc)[0], loc + 2

    @staticmethod
    def _to_row_binary(value: int, dest: bytearray):
        dest += sp('<H', value,)


class Int32(FixedType):
    _array_type = 'i'

    @staticmethod
    def _from_row_binary(source: bytes, loc: int):
        return suf('<i', source, loc)[0], loc + 4

    @staticmethod
    def _to_row_binary(value: int, dest: bytearray):
        dest += sp('<i', value,)


class UInt32(FixedType):
    _array_type = 'I'

    @staticmethod
    def _from_row_binary(source: bytes, loc: int):
        return suf('<I', source, loc)[0], loc + 4

    @staticmethod
    def _to_row_binary(value: int, dest: bytearray):
        dest += sp('<I', value,)


class Int64(FixedType):
    _array_type = 'q'

    @staticmethod
    def _from_row_binary(source: bytes, loc: int):
        return suf('<q', source, loc)[0], loc + 8

    @staticmethod
    def _to_row_binary(value: int, dest: bytearray):
        dest += sp('<q', value,)


class UInt64(FixedType):
    _array_type = 'Q'

    @staticmethod
    def _from_row_binary(source: bytes, loc: int):
        return suf('<q', source, loc)[0], loc + 8

    @staticmethod
    def _to_row_binary(value: int, dest: bytearray):
        dest += sp('<Q', value,)

    @classmethod
    def format(cls, fmt:str):
        fmt = fmt.lower()
        if fmt == 'unsigned':
            cls._array_type = 'Q'
        elif fmt == 'signed':
            cls._array_type = 'q'
        else:
            raise ValueError("Unrecognized UInt64 Output Format")


class BigInt(FixedType, registered=False):
    @staticmethod
    def _to_python_str(column: Sequence):
        return [str(x, 'utf8') for x in column]

    @classmethod
    def format(cls, fmt: str, encoding: str = 'utf8'):
        fmt = fmt.lower()
        if fmt.lower().startswith('str'):
            cls._to_python = cls._to_python_str
            cls._encoding = encoding
        elif fmt.startswith('raw') or fmt.startswith('int') or fmt.startswith('num'):
            cls._to_python = None
        else:
            raise ValueError("Unrecognized BigInt output format")


class Int128(BigInt):
    _byte_size = 16
    _signed = True

    @staticmethod
    def _from_row_binary(source: bytes, loc: int):
        return int.from_bytes(source[loc: loc + 16], 'little', signed=True), loc + 16

    @staticmethod
    def _to_row_binary(value:int, dest: bytearray):
        dest += value.to_bytes(16, 'little')

    def _from_bytes(self, source: Sequence, loc: int, num_rows: int, **_):
        end = loc + 16 * num_rows
        return [int.from_bytes(source[ix:ix + 16], 'little', signed=True) for ix in range(loc, end, 16)], end


class UInt128(BigInt):
    _byte_size = 16
    _signed = False

    @staticmethod
    def _from_row_binary(source: bytes, loc: int):
        return int.from_bytes(source[loc: loc + 16], 'little', signed=False), loc + 16

    @staticmethod
    def _to_row_binary(value:int, dest: bytearray):
        dest += value.to_bytes(16, 'little')

    def _from_bytes(self, source: Sequence, loc: int, num_rows: int, **_):
        end = loc + 16 * num_rows
        return [int.from_bytes(source[ix:ix + 16], 'little', signed=False) for ix in range(loc, end, 16)], end


class Int256(BigInt):
    _byte_size = 32
    _signed = True

    @staticmethod
    def _from_row_binary(source: bytes, loc: int):
        return int.from_bytes(source[loc: loc + 32], 'little', signed=True), loc + 32

    @staticmethod
    def _to_row_binary(value:int, dest: bytearray):
        dest += value.to_bytes(32, 'little')

    def _from_bytes(self, source: Sequence, loc: int, num_rows: int, **_):
        end = loc + 32 * num_rows
        return [int.from_bytes(source[ix:ix + 32], 'little', signed=True) for ix in range(loc, end, 32)], end


class UInt256(BigInt):
    _byte_size = 32
    _signed = False

    @staticmethod
    def _from_row_binary(source: bytes, loc: int):
        return int.from_bytes(source[loc: loc + 32], 'little', signed=False), loc + 32

    @staticmethod
    def _to_row_binary(value:int, dest: bytearray):
        dest += value.to_bytes(32, 'little')

    def _from_bytes(self, source: Sequence, loc: int, num_rows: int, **_):
        end = loc + 32 * num_rows
        return [int.from_bytes(source[ix:ix + 32], 'little', signed=False) for ix in range(loc, end, 32)], end


class Float32(FixedType):
    _array_type = 'f'

    @staticmethod
    def _from_row_binary(source: bytearray, loc: int):
        return suf('f', source, loc)[0], loc + 4

    @staticmethod
    def _to_row_binary(value: float, dest: bytearray):
        dest += sp('f', value,)


class Float64(FixedType):
    _array_type = 'd'

    @staticmethod
    def _from_row_binary(source: bytearray, loc: int):
        return suf('d', source, loc)[0], loc + 8

    @staticmethod
    def _to_row_binary(value: float, dest: bytearray):
        dest += sp('d', (value,))


class Boolean(FixedType):
    _array_type = 'B'

    @staticmethod
    def _from_row_binary(source: bytearray, loc: int):
        return source[loc] > 0, loc + 1

    @staticmethod
    def _to_row_binary(value: bool, dest: bytearray):
        dest += b'\x01' if value else b'\x00'

    @staticmethod
    def _to_python(column: Iterable):
        return [b > 0 for b in column]


class Bool(Boolean):
    pass


class Enum8(FixedType):
    __slots__ = '_name_map', '_int_map'
    _array_type = 'b'

    def __init__(self, type_def: TypeDef):
        super().__init__(type_def)
        escaped_keys = [key.replace("'", "\\'") for key in type_def.keys]
        self._name_map = {key: value for key, value in zip(type_def.keys, type_def.values)}
        self._int_map = {value: key for key, value in zip(type_def.keys, type_def.values)}
        val_str = ', '.join(f"'{key}' = {value}" for key, value in zip(escaped_keys, type_def.values))
        self._name_suffix = f'({val_str})'

    def _from_row_binary(self, source: bytes, loc: int):
        value = source[loc]
        return self._int_map[value if value < 128 else value - 128], loc + 1

    def _to_row_binary(self, value: Union[str, int], dest: bytearray):
        try:
            value = self._name_map[value]
        except KeyError:
            pass
        dest += value if value < 128 else value - 128

    def _to_python(self, column: Sequence):
        lookup = self._int_map
        return [lookup[x] for x in column]

    def _from_python(self, column: Sequence):
        first = self._first_value(column)
        if first is None or isinstance(first, int):
            return column
        lookup = self._name_map.get
        return [lookup(x) for x in column]


class Enum16(Enum8):
    _array_type = 'h'

    def _from_row_binary(self, source: bytes, loc: int):
        return self._int_map[suf('<h', source, loc)[0]], loc + 2

    def _to_row_binary(self, value: Union[str, int], dest: bytearray):
        try:
            value = self._name_map[value]
        except KeyError:
            pass
        dest += sp('<h', value)


class Decimal(FixedType):
    __slots__ = 'scale', 'mult', 'zeros'

    def __init__(self, type_def: TypeDef):
        size = type_def.size
        if size == 0:
            self._name_suffix = type_def.arg_str
            prec = type_def.values[0]
            self.scale = type_def.values[1]
            if prec < 1 or prec > 79:
                raise ArithmeticError(f"Invalid precision {prec} for ClickHouse Decimal type")
            if prec < 10:
                size = 32
            elif prec < 19:
                size = 64
            elif prec < 39:
                size = 128
            else:
                size = 256
        else:
            self.scale = type_def.values[0]
            self._name_suffix = f'{type_def.size}({self.scale})'
        self._byte_size = size // 8
        self.zeros = bytes([0] * self._byte_size)
        self._array_type = array_type(self._byte_size, True)
        self.mult = 10 ** self.scale
        super().__init__(type_def)
        if self._array_type:
            self._to_python = self._to_python_int
        else:
            self._to_python = self._to_python_bytes

    def _from_row_binary(self, source, loc):
        end = loc + self._byte_size
        x = int.from_bytes(source[loc:end], 'little')
        scale = self.scale
        if x >= 0:
            digits = str(x)
            return decimal.Decimal(f'{digits[:-scale]}.{digits[-scale:]}'), end
        digits = str(-x)
        return decimal.Decimal(f'-{digits[:-scale]}.{digits[-scale:]}'), end

    def _to_row_binary(self, value: Any, dest: bytearray):
        if isinstance(value, int) or isinstance(value, float) or (
                isinstance(value, decimal.Decimal) and value.is_finite()):
            dest += int(value * self.mult).to_bytes(self._byte_size, 'little')
        else:
            dest += self.zeros

    def _to_python_int(self, column: Sequence[int]):
        dec = decimal.Decimal
        scale = self.scale
        new_col = []
        app = new_col.append
        for x in column:
            if x >= 0:
                digits = str(x)
                app(dec(f'{digits[:-scale]}.{digits[-scale:]}'))
            else:
                digits = str(-x)
                app(dec(f'-{digits[:-scale]}.{digits[-scale:]}'))
        return new_col

    def _to_python_bytes(self, column: Sequence):
        ifb = partial(int.from_bytes, byteorder='little')
        ints = [ifb(x) for x in column]
        return self._to_python_int(ints)
