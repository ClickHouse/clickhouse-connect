import decimal
from collections.abc import Sequence, MutableSequence
from struct import unpack_from as suf, pack as sp
from typing import Any, Union, Type

from clickhouse_connect.datatypes.base import TypeDef, ArrayType, ClickHouseType
from clickhouse_connect.driver.common import array_type, array_column, write_array, decimal_size, decimal_prec


class Int8(ArrayType):
    _array_type = 'b'
    np_type = 'b'

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


class UInt8(ArrayType):
    _array_type = 'B'
    np_type = 'B'

    @staticmethod
    def _from_row_binary(source: bytes, loc: int):
        return source[loc], loc + 1

    @staticmethod
    def _to_row_binary(value: int, dest: bytearray):
        dest.append(value)


class Int16(ArrayType):
    _array_type = 'h'
    np_type = 'i2'

    @staticmethod
    def _from_row_binary(source: bytes, loc: int):
        return suf('<h', source, loc)[0], loc + 2

    @staticmethod
    def _to_row_binary(value: int, dest: bytearray):
        dest += sp('<h', value, )


class UInt16(ArrayType):
    _array_type = 'H'
    np_type = 'u2'

    @staticmethod
    def _from_row_binary(source: bytes, loc: int):
        return suf('<H', source, loc)[0], loc + 2

    @staticmethod
    def _to_row_binary(value: int, dest: bytearray):
        dest += sp('<H', value, )


class Int32(ArrayType):
    _array_type = 'i'
    np_type = 'i4'

    @staticmethod
    def _from_row_binary(source: bytes, loc: int):
        return suf('<i', source, loc)[0], loc + 4

    @staticmethod
    def _to_row_binary(value: int, dest: bytearray):
        dest += sp('<i', value, )


class UInt32(ArrayType):
    _array_type = 'I'
    np_type = 'u4'

    @staticmethod
    def _from_row_binary(source: bytes, loc: int):
        return suf('<I', source, loc)[0], loc + 4

    @staticmethod
    def _to_row_binary(value: int, dest: bytearray):
        dest += sp('<I', value, )


class Int64(ArrayType):
    _array_type = 'q'
    np_type = 'i8'

    @staticmethod
    def _from_row_binary(source: bytes, loc: int):
        return suf('<q', source, loc)[0], loc + 8

    @staticmethod
    def _to_row_binary(value: int, dest: bytearray):
        dest += sp('<q', value, )


class UInt64(ArrayType):
    _array_type = 'Q'
    np_type = 'u8'

    @staticmethod
    def _from_row_binary(source: bytes, loc: int):
        return suf('<q', source, loc)[0], loc + 8

    @staticmethod
    def _to_row_binary(value: int, dest: bytearray):
        dest += sp('<Q', value, )

    @classmethod
    def format(cls, fmt: str):
        fmt = fmt.lower()
        if fmt == 'unsigned':
            cls._array_type = 'Q'
        elif fmt == 'signed':
            cls._array_type = 'q'
        else:
            raise ValueError("Unrecognized UInt64 Output Format")


class BigInt(ClickHouseType, registered=False):
    _format = 'raw'
    _signed = True
    _byte_size = 0

    @property
    def ch_null(self):
        return bytes(b'\x00' * self._byte_size)

    def _from_native(self, source: MutableSequence, loc: int, num_rows: int, **_):
        signed = self._signed
        sz = self._byte_size
        end = loc + num_rows * sz
        column = []
        app = column.append
        ifb = int.from_bytes
        if self._format == 'string':
            for ix in range(loc, end, sz):
                app(str(ifb(source[ix: ix + sz], 'little', signed=signed)))
        else:
            for ix in range(loc, end, sz):
                app(ifb(source[ix: ix + sz], 'little', signed=signed))
        return column, end

    def _to_native(self, column: Sequence, dest: MutableSequence, **_):
        first = self._first_value(column)
        if not column:
            return column
        sz = self._byte_size
        signed = self._signed
        empty = self.ch_null
        ext = dest.extend
        if isinstance(first, str):
            if self.nullable:
                for x in column:
                    if x:
                        ext(int(x).to_bytes(sz, 'little', signed=signed))
                    else:
                        ext(empty)
            else:
                for x in column:
                    ext(int(x).to_bytes(sz, 'little', signed=signed))
        else:
            if self.nullable:
                for x in column:
                    if x:
                        ext(x.to_bytes(sz, 'little', signed=signed))
                    else:
                        ext(empty)
            else:
                for x in column:
                    ext(x.to_bytes(sz, 'little', signed=signed))

    @classmethod
    def format(cls, fmt: str):
        fmt = fmt.lower()
        if fmt.lower().startswith('str'):
            cls._format = 'string'
        else:
            cls._format = 'raw'


class Int128(BigInt):
    _byte_size = 16
    _signed = True

    @staticmethod
    def _from_row_binary(source: bytes, loc: int):
        return int.from_bytes(source[loc: loc + 16], 'little', signed=True), loc + 16

    @staticmethod
    def _to_row_binary(value: int, dest: bytearray):
        dest += value.to_bytes(16, 'little')


class UInt128(BigInt):
    _byte_size = 16
    _signed = False

    @staticmethod
    def _from_row_binary(source: bytes, loc: int):
        return int.from_bytes(source[loc: loc + 16], 'little', signed=False), loc + 16

    @staticmethod
    def _to_row_binary(value: int, dest: bytearray):
        dest += value.to_bytes(16, 'little')


class Int256(BigInt):
    _byte_size = 32
    _signed = True

    @staticmethod
    def _from_row_binary(source: bytes, loc: int):
        return int.from_bytes(source[loc: loc + 32], 'little', signed=True), loc + 32

    @staticmethod
    def _to_row_binary(value: int, dest: bytearray):
        dest += value.to_bytes(32, 'little')


class UInt256(BigInt):
    _byte_size = 32
    _signed = False

    @staticmethod
    def _from_row_binary(source: bytes, loc: int):
        return int.from_bytes(source[loc: loc + 32], 'little'), loc + 32

    @staticmethod
    def _to_row_binary(value: int, dest: bytearray):
        dest += value.to_bytes(32, 'little')


class Float32(ArrayType):
    _array_type = 'f'
    np_type = 'f4'

    @staticmethod
    def _from_row_binary(source: bytearray, loc: int):
        return suf('f', source, loc)[0], loc + 4

    @staticmethod
    def _to_row_binary(value: float, dest: bytearray):
        dest += sp('f', value, )


class Float64(ArrayType):
    _array_type = 'd'
    np_type = 'f8'

    @staticmethod
    def _from_row_binary(source: bytearray, loc: int):
        return suf('d', source, loc)[0], loc + 8

    @staticmethod
    def _to_row_binary(value: float, dest: bytearray):
        dest += sp('d', (value,))


class Boolean(ClickHouseType):
    np_type = '?'

    @staticmethod
    def _from_row_binary(source: bytearray, loc: int):
        return source[loc] > 0, loc + 1

    @staticmethod
    def _to_row_binary(value: bool, dest: bytearray):
        dest += b'\x01' if value else b'\x00'

    @staticmethod
    def _from_native(source: Sequence, loc: int, num_rows: int, **_):
        column, loc = array_column('B', source, loc, num_rows)
        return [b > 0 for b in column], loc

    @staticmethod
    def _to_native(column: Sequence, dest: MutableSequence, **_):
        write_array('B', [1 if x else 0 for x in column], dest)


class Bool(Boolean):
    pass


class Enum8(ArrayType):
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

    def _from_native(self, source: Sequence, loc: int, num_rows: int, **_):
        column, loc = array_column(self._array_type, source, loc, num_rows)
        lookup = self._int_map.get
        return [lookup(x, None) for x in column], loc

    def _to_native(self, column: Sequence, dest: MutableSequence, **_):
        first = self._first_value(column)
        if first is None or isinstance(first, int):
            if self.nullable:
                column = [0 if not x else x for x in column]
            write_array(self._array_type, column, dest)
        else:
            lookup = self._name_map.get
            write_array(self._array_type, [lookup(x, 0) for x in column], dest)


class Enum16(Enum8):
    _array_type = 'h'

    def _from_row_binary(self, source: bytes, loc: int):
        return self._int_map[suf('<h', source, loc)[0]], loc + 2

    def _to_row_binary(self, value: Union[str, int], dest: bytearray):
        try:
            value = self._name_map[value]
        except KeyError:
            value = 0
        dest += sp('<h', value)


class Decimal(ClickHouseType):
    __slots__ = 'prec', 'scale', '_mult', '_zeros'

    @classmethod
    def build(cls: Type['ClickHouseType'], type_def: TypeDef):
        if type_def in cls._instance_cache:
            return cls._instance_cache[type_def]
        size = type_def.size
        if size == 0:
            prec = type_def.values[0]
            scale = type_def.values[1]
            size = decimal_size(prec)
        else:
            prec = decimal_prec[size]
            scale = type_def.values[0]
        type_cls = BigDecimal if size > 64 else Decimal
        return cls._instance_cache.setdefault(type_def, type_cls(type_def, prec, size, scale))

    def __init__(self, type_def: TypeDef, prec, size, scale):
        super().__init__(type_def)
        self.prec = prec
        self.scale = scale
        self._mult = 10 ** scale
        self._byte_size = size // 8
        self._zeros = bytes([0] * self._byte_size)
        self._name_suffix = f'({prec}, {scale})'
        self._array_type = array_type(self._byte_size, True)

    @property
    def ch_null(self):
        return self._zeros

    def _from_row_binary(self, source, loc):
        end = loc + self._byte_size
        x = int.from_bytes(source[loc:end], 'little', signed=True)
        scale = self.scale
        if x >= 0:
            digits = str(x)
            return decimal.Decimal(f'{digits[:-scale]}.{digits[-scale:]}'), end
        digits = str(-x)
        return decimal.Decimal(f'-{digits[:-scale]}.{digits[-scale:]}'), end

    def _to_row_binary(self, value: Any, dest: bytearray):
        if isinstance(value, int) or isinstance(value, float) or (
                isinstance(value, decimal.Decimal) and value.is_finite()):
            dest += int(value * self._mult).to_bytes(self._byte_size, 'little')
        else:
            dest += self._zeros

    def _from_native(self, source: Sequence, loc: int, num_rows: int, **_):
        column, loc = array_column(self._array_type, source, loc, num_rows)
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
        return new_col, loc

    def _to_native(self, column: Sequence, dest: MutableSequence, **_):
        mult = self._mult
        if self.nullable:
            write_array(self._array_type, [int(x * mult) if x else 0 for x in column], dest)
        else:
            write_array(self._array_type, [int(x * mult) for x in column], dest)


class BigDecimal(Decimal, registered=False):
    def _from_native(self, source: Sequence, loc: int, num_rows: int, **_):
        dec = decimal.Decimal
        scale = self.scale
        column = []
        app = column.append
        sz = self._byte_size
        end = loc + sz * num_rows
        ifb = int.from_bytes
        for ix in range(loc, end, sz):
            x = ifb(source[ix: ix + sz], 'little', signed=True)
            if x >= 0:
                digits = str(x)
                app(dec(f'{digits[:-scale]}.{digits[-scale:]}'))
            else:
                digits = str(-x)
                app(dec(f'-{digits[:-scale]}.{digits[-scale:]}'))
        return column, end

    def _to_native(self, column: Sequence, dest: MutableSequence, **_):
        mult = self._mult
        sz = self._byte_size
        itb = int.to_bytes
        if self.nullable:
            nv = self._zeros
            for x in column:
                dest += nv if not x else itb(int(x * mult), sz, 'little', signed=True)
        else:
            for x in column:
                dest += itb(int(x * mult), sz, 'little', signed=True)
