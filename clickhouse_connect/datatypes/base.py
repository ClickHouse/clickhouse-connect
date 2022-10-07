import array
import logging

from abc import ABC
from math import log
from typing import NamedTuple, Dict, Type, Any, Sequence, MutableSequence, Optional, Union, Tuple

from clickhouse_connect.driver.common import array_column, array_type, int_size, read_uint64, write_array, \
    write_uint64, low_card_version
from clickhouse_connect.driver.exceptions import NotSupportedError
from clickhouse_connect.driver.threads import query_settings

logger = logging.getLogger(__name__)
ch_read_formats = {}
ch_write_formats = {}


class TypeDef(NamedTuple):
    """
    Immutable tuple that contains all additional information needed to construct a particular ClickHouseType
    """
    wrappers: tuple = ()
    keys: tuple = ()
    values: tuple = ()

    @property
    def arg_str(self):
        return f"({', '.join(str(v) for v in self.values)})" if self.values else ''


class ClickHouseType(ABC):
    """
    Base class for all ClickHouseType objects.
    """
    __slots__ = 'nullable', 'low_card', 'wrappers', 'type_def', '__dict__'
    _ch_name = None
    _name_suffix = ''
    _encoding = 'utf8'
    np_type = 'O'  # Default to Numpy Object type
    valid_formats = 'native'

    python_null = 0
    python_type = None

    def __init_subclass__(cls, registered: bool = True):
        if registered:
            cls._ch_name = cls.__name__
            type_map[cls._ch_name] = cls

    @classmethod
    def build(cls: Type['ClickHouseType'], type_def: TypeDef):
        return cls(type_def)

    @classmethod
    def _active_format(cls, fmt_map: Dict[Type['ClickHouseType'], str]):
        overrides = getattr(query_settings, 'column_overrides', None)
        if overrides and cls in overrides:
            return overrides[cls]
        overrides = getattr(query_settings, 'query_overrides', None)
        if overrides and cls in overrides:
            return overrides[cls]
        return fmt_map.get(cls, 'native')

    @classmethod
    def read_format(cls):
        return cls._active_format(ch_read_formats)

    @classmethod
    def write_format(cls):
        return cls._active_format(ch_write_formats)

    def __init__(self, type_def: TypeDef):
        """
        Base class constructor that sets Nullable and LowCardinality wrappers
        :param type_def:  ClickHouseType base configuration parameters
        """
        self.type_def = type_def
        self.wrappers = type_def.wrappers
        self.low_card = 'LowCardinality' in self.wrappers
        self.nullable = 'Nullable' in self.wrappers

    def __eq__(self, other):
        return other.__class__ == self.__class__ and self.type_def == other.type_def

    def __hash__(self):
        return hash((self.type_def, self.__class__))

    @property
    def name(self):
        name = f'{self._ch_name}{self._name_suffix}'
        for wrapper in reversed(self.wrappers):
            name = f'{wrapper}({name})'
        return name

    @property
    def encoding(self):
        query_encoding = getattr(query_settings, 'query_encoding', None)
        return query_encoding or self._encoding

    def write_native_prefix(self, dest: MutableSequence):
        """
        Prefix is primarily used is for the LowCardinality version (but see the JSON data type).  Because of the
        way the ClickHouse C++ code is written, this must be done before any data is written even if the
        LowCardinality column is within a container.  The only recognized low cardinality version is 1
        :param dest: The native protocol binary write buffer
        """
        if self.low_card:
            write_uint64(low_card_version, dest)

    def read_native_prefix(self, source: Sequence, loc: int):
        """
        Read the low cardinality version.  Like the write method, this has to happen immediately for container classes
        :param source: The native protocol binary read buffer
        :param loc: Moving location pointer for the read buffer
        :return: updated read pointer
        """
        if self.low_card:
            v, loc = read_uint64(source, loc)
            if v != low_card_version:
                logger.warning('Unexpected low cardinality version %d reading type %s', v, self.name)
        return loc

    def read_native_column(self, source: Sequence, loc: int, num_rows: int, **kwargs) -> Tuple[Sequence, int]:
        """
        Wrapping read method for all ClickHouseType data types.  Only overridden for container classes so that the LowCardinality version
        is read for the contained types
        :param source: Native protocol binary read buffer
        :param loc: Moving location for the read buffer
        :param num_rows: Number of rows expected in the column
        :param kwargs: Pass any extra keyword arguments to the main read_native_data function
        :return: The decoded column data as a sequence and the updated location pointer
        """
        loc = self.read_native_prefix(source, loc)
        return self.read_native_data(source, loc, num_rows, **kwargs)

    def read_native_data(self, source: Sequence, loc: int, num_rows: int, use_none=True) -> Tuple[Sequence, int]:
        """
        Public read method for all ClickHouseType data type columns.
        :param source: Native protocol binary read buffer
        :param loc: Moving location for the read buffer
        :param num_rows: Number of rows expected in the column
        :param use_none: Use the Python None type for ClickHouse nulls.  Otherwise use the empty or zero type.
         Allows support for pandas data frames that do not support None
        :return: The decoded column plust the updated location pointer
        """
        if self.low_card:
            return self._read_native_low_card(source, loc, num_rows, use_none)
        if self.nullable:
            null_map = memoryview(source[loc: loc + num_rows])
            loc += num_rows
            column, loc = self._read_native_binary(source, loc, num_rows)
            if use_none:
                if isinstance(column, (tuple, array.array)):
                    return [None if null_map[ix] else column[ix] for ix in range(num_rows)], loc
                for ix in range(num_rows):
                    if null_map[ix]:
                        column[ix] = None
            return column, loc
        return self._read_native_binary(source, loc, num_rows)

    # These two methods are really abstract, but they aren't implemented for container classes which
    # delegate binary operations to their elements

    # pylint: disable=no-self-use
    def _read_native_binary(self, _source: Sequence, _loc: int, _num_rows: int) \
            -> Tuple[Union[Sequence, MutableSequence], int]:
        """
        Lowest level read method for ClickHouseType native data columns
        :param _source: Native protocol binary read buffer
        :param _loc: Read pointer in the binary read buffer
        :param _num_rows: Expected number of rows in the column
        :return: Decoded column plus updated read buffer
        """
        return [], 0

    def _write_native_binary(self, column: Union[Sequence, MutableSequence], dest: MutableSequence):
        """
        Lowest level write method for ClickHouseType data columns
        :param column: Python data column
        :param dest: Native protocol write buffer
        """

    def write_native_column(self, column: Sequence, dest: MutableSequence):
        """
        Wrapping write method for ClickHouseTypes.  Only overridden for container types that so that
        the write_native_prefix is done at the right time for contained types
        :param column: Column/sequence of Python values to write
        :param dest: Native binary write buffer
        """
        self.write_native_prefix(dest)
        self.write_native_data(column, dest)

    def write_native_data(self, column: Sequence, dest: MutableSequence):
        """
        Public native write method for ClickHouseTypes.  Delegates the actual write to either the LowCardinality
        write method or the _write_native_binary method of the type
        :param column: Sequence of Python data
        :param dest: Native binary write buffer
        """
        if self.low_card:
            self._write_native_low_card(column, dest)
        else:
            if self.nullable:
                dest += bytes([1 if x is None else 0 for x in column])
            self._write_native_binary(column, dest)

    def _read_native_low_card(self, source: Sequence, loc: int, num_rows: int, use_none=True):
        if num_rows == 0:
            return tuple(), loc
        key_data, loc = read_uint64(source, loc)
        index_sz = 2 ** (key_data & 0xff)
        key_cnt, loc = read_uint64(source, loc)
        keys, loc = self._read_native_binary(source, loc, key_cnt)
        if self.nullable:
            try:
                keys[0] = None if use_none else self.python_null
            except TypeError:
                keys = (None if use_none else self.python_null,) + tuple(keys[1:])
        index_cnt, loc = read_uint64(source, loc)
        assert index_cnt == num_rows
        index, loc = array_column(array_type(index_sz, False), source, loc, num_rows)
        return tuple(keys[ix] for ix in index), loc

    def _write_native_low_card(self, column: Sequence, dest: MutableSequence):
        if not column:
            return
        index = []
        keys = []
        rev_map = {}
        rmg = rev_map.get
        if self.nullable:
            keys.append(None)
            key = 1
            for x in column:
                if x is None:
                    index.append(0)
                else:
                    ix = rmg(x)
                    if ix is None:
                        index.append(key)
                        keys.append(x)
                        rev_map[x] = key
                        key += 1
                    else:
                        index.append(ix)
        else:
            key = 0
            for x in column:
                ix = rmg(x)
                if ix is None:
                    index.append(key)
                    keys.append(x)
                    rev_map[x] = key
                    key += 1
                else:
                    index.append(ix)
        ix_type = int(log(len(keys), 2)) >> 3  # power of two bytes needed to store the total number of keys
        write_uint64((1 << 9) | (1 << 10) | ix_type, dest)  # Index type plus new dictionary (9) and additional keys(10)
        write_uint64(len(keys), dest)
        self._write_native_binary(keys, dest)
        write_uint64(len(index), dest)
        write_array(array_type(1 << ix_type, False), index, dest)

    def _first_value(self, column: Sequence) -> Optional[Any]:
        if self.nullable:
            return next((x for x in column if x is not None), None)
        if column:
            return column[0]
        return None


EMPTY_TYPE_DEF = TypeDef()
NULLABLE_TYPE_DEF = TypeDef(wrappers=('Nullable',))
LC_TYPE_DEF = TypeDef(wrappers=('LowCardinality',))
type_map: Dict[str, Type[ClickHouseType]] = {}


class ArrayType(ClickHouseType, ABC, registered=False):
    """
    ClickHouse type that utilizes Python array.array for fast reads and writes of binary data.  array.array can only be used for
    ClickHouse types that can be translated into UInt64 or small integers, or Float32/64
    """
    _signed = True
    _array_type = None
    _struct_type = None
    valid_formats = 'string', 'native'
    python_type = int

    def __init_subclass__(cls, registered: bool = True):
        super().__init_subclass__(registered)
        if cls._array_type in ('i', 'I') and int_size == 2:
            cls._array_type = 'L' if cls._array_type.isupper() else 'l'
        if isinstance(cls._array_type, str) and cls._array_type:
            cls._struct_type = '<' + cls._array_type

    def _read_native_binary(self, source: Sequence, loc: int, num_rows: int):
        column, loc =  array_column(self._array_type, source, loc, num_rows)
        if self.read_format() == 'string':
            column = [str(x) for x in column]
        return column, loc

    def _write_native_binary(self, column: Union[Sequence, MutableSequence], dest: MutableSequence):
        if column and self.nullable:
            first = column[0]
            try:
                column[0] = None
                for ix, x in enumerate(column):
                    if not x:
                        column[ix] = 0
                column[0] = first or 0
            except TypeError:
                column = [0 if x is None else x for x in column]
        write_array(self._array_type, column, dest)


class UnsupportedType(ClickHouseType, ABC, registered=False):
    """
    Base class for ClickHouse types that can't be serialized/deserialized into Python types.
    Mostly useful just for DDL statements
    """
    def __init__(self, type_def: TypeDef):
        super().__init__(type_def)
        self._name_suffix = type_def.arg_str

    def _read_native_binary(self, source: Sequence, loc: int, num_rows: int):
        raise NotSupportedError(f'{self.name} deserialization not supported')

    def _write_native_binary(self, column: Union[Sequence, MutableSequence], dest: MutableSequence):
        raise NotSupportedError(f'{self.name} serialization  not supported')
