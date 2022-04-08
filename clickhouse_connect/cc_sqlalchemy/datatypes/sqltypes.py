import pytz
from collections.abc import Sequence
from enum import Enum as PyEnum
from typing import Type, Union

from sqlalchemy.types import Integer, Float, Numeric, Boolean as SqlaBoolean, \
    UserDefinedType, String as SqlaString, DateTime as SqlaDateTime, Date as SqlaDate
from sqlalchemy.exc import ArgumentError

from clickhouse_connect.cc_sqlalchemy.datatypes.base import ChSqlaType
from clickhouse_connect.datatypes.base import TypeDef, NULLABLE_TYPE_DEF, LC_TYPE_DEF, EMPTY_TYPE_DEF
from clickhouse_connect.datatypes.numeric import Enum8 as ChEnum8, Enum16 as ChEnum16
from clickhouse_connect.driver.common import decimal_prec


class Int8(ChSqlaType, Integer):
    pass


class UInt8(ChSqlaType, Integer):
    pass


class Int16(ChSqlaType, Integer):
    pass


class UInt16(ChSqlaType, Integer):
    pass


class Int32(ChSqlaType, Integer):
    pass


class UInt32(ChSqlaType, Integer):
    pass


class Int64(ChSqlaType, Integer):
    pass


class UInt64(ChSqlaType, Integer):
    pass


class Int128(ChSqlaType, Integer):
    pass


class UInt128(ChSqlaType, Integer):
    pass


class Int256(ChSqlaType, Integer):
    pass


class UInt256(ChSqlaType, Integer):
    pass


class Float32(ChSqlaType, Float):
    def __init__(self, type_def: TypeDef = EMPTY_TYPE_DEF):
        ChSqlaType.__init__(self, type_def)
        Float.__init__(self)


class Float64(ChSqlaType, Float):
    def __init__(self, type_def: TypeDef = EMPTY_TYPE_DEF):
        ChSqlaType.__init__(self, type_def)
        Float.__init__(self)


class Bool(ChSqlaType, SqlaBoolean):
    def __init__(self, type_def: TypeDef = EMPTY_TYPE_DEF):
        ChSqlaType.__init__(self, type_def)
        SqlaBoolean.__init__(self)


class Boolean(Bool):
    pass


class Decimal(ChSqlaType, Numeric):
    def __init__(self, precision: int = 0, scale: int = 0, type_def: TypeDef = None):
        if type_def:
            if type_def.size:
                precision = decimal_prec[type_def.size]
                scale = type_def.values[0]
            else:
                precision, scale = type_def.values
        elif not precision or not scale:
            raise ArgumentError('Precision and scale required for ClickHouse Decimal type')
        else:
            type_def = TypeDef(values=(precision, scale))
        ChSqlaType.__init__(self, type_def)
        Numeric.__init__(self, precision, scale)


class Enum(ChSqlaType, UserDefinedType):
    _size = 16
    python_type = str

    def __init__(self, enum: Type[PyEnum] = None, keys: Sequence[str] = None, values: Sequence[int] = None,
                 type_def: TypeDef = None):
        if not type_def:
            if enum:
                keys = [e.name for e in enum]
                values = [e.value for e in enum]
            self._validate(keys, values)
            if self.__class__.__name__ == 'Enum':
                if max(values) <= 127 and min(values) >= -128 :
                    self._ch_type_cls = ChEnum8
                else:
                    self._ch_type_cls = ChEnum16
            type_def = TypeDef(keys=tuple(keys), values=tuple(values))
        super().__init__(type_def)

    @classmethod
    def _validate(cls, keys: Sequence, values: Sequence):
        bad_key = next((x for x in keys if not isinstance(x, str)), None)
        if bad_key:
            raise ArgumentError(f'ClickHouse enum key {bad_key} is not a string')
        bad_value = next((x for x in values if not isinstance(x, int)), None)
        if bad_value:
            raise ArgumentError(f'ClickHouse enum value {bad_value} is not an integer')
        value_min = -(2 ** (cls._size - 1))
        value_max = 2 ** (cls._size - 1) - 1
        bad_value = next((x for x in values if x < value_min or x > value_max), None)
        if bad_value:
            raise ArgumentError(f'Clickhouse enum value {bad_value} is out of range')


class Enum8(Enum):
    _size = 8
    _ch_type_cls = ChEnum8


class Enum16(Enum):
    _ch_type_cls = ChEnum16


class String(ChSqlaType, SqlaString):
    pass


class FixedString(ChSqlaType, SqlaString):
    def __init__(self, size: int = -1, type_def: TypeDef = None):
        if not type_def:
            type_def = TypeDef(values=(size,))
        ChSqlaType.__init__(self, type_def)
        SqlaString.__init__(self, size)


class IPv4(ChSqlaType, UserDefinedType):
    python_type = None


class IPv6(ChSqlaType, UserDefinedType):
    python_type = None


class UUID(ChSqlaType, UserDefinedType):
    python_type = None


class Date(ChSqlaType, SqlaDate):
    pass


class Date32(ChSqlaType, SqlaDate):
    pass


class DateTime(ChSqlaType, SqlaDateTime):
    def __init__(self, tz: str = None, type_def: TypeDef = None):
        if not type_def:
            if tz:
                pytz.timezone(tz)
                type_def = TypeDef(values=(f"'{tz}'",))
            else:
                type_def = EMPTY_TYPE_DEF
        ChSqlaType.__init__(self, type_def)
        SqlaDateTime.__init__(self)


class DateTime64(ChSqlaType, SqlaDateTime):
    def __init__(self, precision: int = None, tz: str = None, type_def: TypeDef = None):
        if not type_def:
            if tz:
                pytz.timezone(tz)
                type_def = TypeDef(values=(precision, f"'{tz}'"))
            else:
                type_def = TypeDef(values=(precision,))
        prec = type_def.values[0] if len(type_def.values) else None
        if not isinstance(prec, int) or prec < 0 or prec > 9:
            raise ArgumentError(f'Invalid precision value {prec} for ClickHouse DateTime64')
        ChSqlaType.__init__(self, type_def)
        SqlaDateTime.__init__(self)


class Nullable:
    def __new__(cls, element: Union[ChSqlaType, Type[ChSqlaType]]):
        if callable(element):
            return element(type_def=NULLABLE_TYPE_DEF)
        if element.low_card:
            raise ArgumentError('Low Cardinality type cannot be Nullable')
        orig = element.type_def
        wrappers = orig if 'Nullable' in orig.wrappers else orig.wrappers + ('Nullable',)
        return element.__class__(type_def=TypeDef(orig.size, wrappers, orig.keys, orig.values))


class LowCardinality:
    def __new__(cls, element: Union[ChSqlaType, Type[ChSqlaType]]):
        if callable(element):
            return element(type_def=LC_TYPE_DEF)
        orig = element.type_def
        wrappers = orig if 'LowCardinality' in orig.wrappers else ('LowCardinality',) + orig.wrappers
        return element.__class__(type_def=TypeDef(orig.size, wrappers, orig.keys, orig.values))


class Array(ChSqlaType, UserDefinedType):
    python_type = list

    def __init__(self, element: Union[ChSqlaType, Type[ChSqlaType]] = None, type_def: TypeDef = None):
        if not type_def:
            if callable(element):
                element = element()
            type_def = TypeDef(values=(element.name,))
        super().__init__(type_def)


class Map(ChSqlaType, UserDefinedType):
    python_type = dict

    def __init__(self, key_type: Union[ChSqlaType, Type[ChSqlaType]] = None,
                 value_type: Union[ChSqlaType, Type[ChSqlaType]] = None, type_def: TypeDef = None):
        if not type_def:
            if callable(key_type):
                key_type = key_type()
            if callable(value_type):
                value_type = value_type()
            type_def = TypeDef(values=(key_type.name, value_type.name))
        super().__init__(type_def)


class Tuple(ChSqlaType, UserDefinedType):
    python_type = tuple

    def __init__(self, elements: Sequence[Union[ChSqlaType, Type[ChSqlaType]]] = None, type_def: TypeDef = None):
        if not type_def:
            values = [et() if callable(et) else et for et in elements]
            type_def = TypeDef(values=tuple(v.name for v in values))
        super().__init__(type_def)


class JSON(ChSqlaType, UserDefinedType):
    python_type = None


class Nested(ChSqlaType, UserDefinedType):
    python_type = None


class Object(ChSqlaType, UserDefinedType):
    python_type = None

    def __init__(self, fmt: str = None, type_def: TypeDef = None):
        if not type_def:
            type_def = TypeDef(values=(fmt,))
        super().__init__(type_def)


class SimpleAggregateFunction(ChSqlaType, UserDefinedType):
    python_type = None

    def __init__(self, element: Union[ChSqlaType, Type[ChSqlaType]] = None, type_def: TypeDef = None):
        if not type_def:
            if callable(element):
                element = element()
            type_def = TypeDef(values=(element.name,))
        super().__init__(type_def)


class AggregateFunction(ChSqlaType, UserDefinedType):
    python_type = None

    def __init__(self, *params, type_def: TypeDef = None):
        if not type_def:
            values = ()
            for x in params:
                if callable(x):
                    x = x()
                if isinstance(x, ChSqlaType):
                    x = x.name
                values += (x,)
            type_def = TypeDef(values=values)
        super().__init__(type_def)
