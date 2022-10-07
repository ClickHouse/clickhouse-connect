import decimal
from typing import Union, Type, Sequence, MutableSequence

from clickhouse_connect.datatypes.base import TypeDef, ArrayType, ClickHouseType
from clickhouse_connect.driver.common import array_type, array_column, write_array, decimal_size, decimal_prec


class Int8(ArrayType):
    _array_type = 'b'
    np_type = 'b'


class UInt8(ArrayType):
    _array_type = 'B'
    np_type = 'B'


class Int16(ArrayType):
    _array_type = 'h'
    np_type = 'i2'


class UInt16(ArrayType):
    _array_type = 'H'
    np_type = 'u2'


class Int32(ArrayType):
    _array_type = 'i'
    np_type = 'i4'


class UInt32(ArrayType):
    _array_type = 'I'
    np_type = 'u4'


class Int64(ArrayType):
    _array_type = 'q'
    np_type = 'i8'


class UInt64(ArrayType):
    valid_formats = 'signed', 'native'

    @property
    def _array_type(self):
        return 'Q' if self.read_format() == 'unsigned' else 'q'

    @property
    def np_type(self):
        return 'u8' if self.read_format() == 'unsigned' else 'q'


class BigInt(ClickHouseType, registered=False):
    _signed = True
    _byte_size = 0
    valid_formats = 'string', 'native'

    def _read_native_binary(self, source: Sequence, loc: int, num_rows: int):
        signed = self._signed
        sz = self._byte_size
        end = loc + num_rows * sz
        column = []
        app = column.append
        ifb = int.from_bytes
        if self.read_format() == 'string':
            for ix in range(loc, end, sz):
                app(str(ifb(source[ix: ix + sz], 'little', signed=signed)))
        else:
            for ix in range(loc, end, sz):
                app(ifb(source[ix: ix + sz], 'little', signed=signed))
        return column, end

    # pylint: disable=too-many-branches
    def _write_native_binary(self, column: Union[Sequence, MutableSequence], dest: MutableSequence):
        first = self._first_value(column)
        if not column:
            return
        sz = self._byte_size
        signed = self._signed
        empty = bytes(b'\x00' * sz)
        ext = dest.extend
        if isinstance(first, str) or self.write_format() == 'string':
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


class Int128(BigInt):
    _byte_size = 16
    _signed = True


class UInt128(BigInt):
    _byte_size = 16
    _signed = False


class Int256(BigInt):
    _byte_size = 32
    _signed = True


class UInt256(BigInt):
    _byte_size = 32
    _signed = False


class Float32(ArrayType):
    _array_type = 'f'
    np_type = 'f4'
    python_type = float


class Float64(ArrayType):
    _array_type = 'd'
    np_type = 'f8'
    python_type = float


class Bool(ClickHouseType):
    np_type = '?'
    python_type = bool

    def _read_native_binary(self, source: Sequence, loc: int, num_rows: int):
        column, loc = array_column('B', source, loc, num_rows)
        return [b > 0 for b in column], loc

    def _write_native_binary(self, column: Union[Sequence, MutableSequence], dest: MutableSequence):
        write_array('B', [1 if x else 0 for x in column], dest)


class Boolean(Bool):
    pass


class Enum(ArrayType):
    __slots__ = '_name_map', '_int_map'
    _array_type = 'b'
    python_type = str

    def __init__(self, type_def: TypeDef):
        super().__init__(type_def)
        escaped_keys = [key.replace("'", "\\'") for key in type_def.keys]
        self._name_map = dict(zip(type_def.keys, type_def.values))
        self._int_map = dict(zip(type_def.values, type_def.keys))
        val_str = ', '.join(f"'{key}' = {value}" for key, value in zip(escaped_keys, type_def.values))
        self._name_suffix = f'({val_str})'

    def _read_native_binary(self, source: Sequence, loc: int, num_rows: int):
        column, loc = array_column(self._array_type, source, loc, num_rows)
        lookup = self._int_map.get
        return [lookup(x, None) for x in column], loc

    def _write_native_binary(self, column: Union[Sequence, MutableSequence], dest: MutableSequence):
        first = self._first_value(column)
        if first is None or isinstance(first, int):
            if self.nullable:
                column = [0 if not x else x for x in column]
            write_array(self._array_type, column, dest)
        else:
            lookup = self._name_map.get
            write_array(self._array_type, [lookup(x, 0) for x in column], dest)


class Enum8(Enum):
    _array_type = 'b'


class Enum16(Enum):
    _array_type = 'h'


class Decimal(ClickHouseType):
    __slots__ = 'prec', 'scale', '_mult', '_zeros', '_byte_size', '_array_type'
    python_type = decimal.Decimal
    dec_size = 0

    @classmethod
    def build(cls: Type['Decimal'], type_def: TypeDef):
        size = cls.dec_size
        if size == 0:
            prec = type_def.values[0]
            scale = type_def.values[1]
            size = decimal_size(prec)
        else:
            prec = decimal_prec[size]
            scale = type_def.values[0]
        type_cls = BigDecimal if size > 64 else Decimal
        return type_cls(type_def, prec, size, scale)

    def __init__(self, type_def: TypeDef, prec, size, scale):
        super().__init__(type_def)
        self.prec = prec
        self.scale = scale
        self._mult = 10 ** scale
        self._byte_size = size // 8
        self._zeros = bytes([0] * self._byte_size)
        self._name_suffix = f'({prec}, {scale})'
        self._array_type = array_type(self._byte_size, True)

    def _read_native_binary(self, source: Sequence, loc: int, num_rows: int):
        column, loc = array_column(self._array_type, source, loc, num_rows)
        dec = decimal.Decimal
        scale = self.scale
        prec = self.prec
        if scale == 0:
            return [dec(str(x)) for x in column], loc
        new_col = []
        app = new_col.append
        for x in column:
            if x >= 0:
                digits = str(x).rjust(prec, '0')
                app(dec(f'{digits[:-scale]}.{digits[-scale:]}'))
            else:
                digits = str(-x).rjust(prec, '0')
                app(dec(f'-{digits[:-scale]}.{digits[-scale:]}'))
        return new_col, loc

    def _write_native_binary(self, column: Union[Sequence, MutableSequence], dest: MutableSequence):
        mult = self._mult
        if self.nullable:
            write_array(self._array_type, [int(x * mult) if x else 0 for x in column], dest)
        else:
            write_array(self._array_type, [int(x * mult) for x in column], dest)


class BigDecimal(Decimal, registered=False):
    def _read_native_binary(self, source: Sequence, loc: int, num_rows: int):
        dec = decimal.Decimal
        scale = self.scale
        prec = self.prec
        column = []
        app = column.append
        sz = self._byte_size
        end = loc + sz * num_rows
        ifb = int.from_bytes
        if scale == 0:
            for ix in range(loc, end, sz):
                app(dec(str(ifb(source[ix: ix + sz], 'little', signed=True))))
            return column, end
        for ix in range(loc, end, sz):
            x = ifb(source[ix: ix + sz], 'little', signed=True)
            if x >= 0:
                digits = str(x).rjust(prec, '0')
                app(dec(f'{digits[:-scale]}.{digits[-scale:]}'))
            else:
                digits = str(-x).rjust(prec, '0')
                app(dec(f'-{digits[:-scale]}.{digits[-scale:]}'))
        return column, end

    def _write_native_binary(self, column: Union[Sequence, MutableSequence], dest: MutableSequence):
        with decimal.localcontext() as ctx:
            ctx.prec = self.prec
            mult = decimal.Decimal(f"{self._mult}.{'0' * self.scale}")
            sz = self._byte_size
            itb = int.to_bytes
            if self.nullable:
                v = self._zeros
                for x in column:
                    dest += v if not x else itb(int(x * mult), sz, 'little', signed=True)
            else:
                for x in column:
                    dest += itb(int(x * mult), sz, 'little', signed=True)


class Decimal32(Decimal):
    dec_size = 32


class Decimal64(Decimal):
    dec_size = 64


class Decimal128(Decimal):
    dec_size = 128


class Decimal256(Decimal):
    dec_size = 256
