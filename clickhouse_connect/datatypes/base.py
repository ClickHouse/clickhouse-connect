import array
from abc import abstractmethod, ABC
from math import log
from typing import NamedTuple, Dict, Type, Any, Sequence, MutableSequence, Optional, Union, Tuple

from clickhouse_connect.driver.common import array_column, array_type, int_size, read_uint64, write_array, \
    write_uint64, low_card_version
from clickhouse_connect.driver.exceptions import NotSupportedError


class TypeDef(NamedTuple):
    size: int = 0
    wrappers: tuple = ()
    keys: tuple = ()
    values: tuple = ()
    format: str = None

    @property
    def arg_str(self):
        return f"({', '.join(str(v) for v in self.values)})" if self.values else ''


class ClickHouseType(ABC):
    __slots__ = 'nullable', 'low_card', 'wrappers', 'format', '__dict__'
    _ch_name = None
    _instance_cache: Dict[TypeDef, 'ClickHouseType'] = {}
    _name_suffix = ''
    np_type = 'O'
    python_null = 0
    python_type = None

    def __init_subclass__(cls, registered: bool = True):
        if registered:
            cls._ch_name = cls.__name__
            cls._instance_cache = {}
            type_map[cls._ch_name.upper()] = cls

    @classmethod
    def build(cls: Type['ClickHouseType'], type_def: TypeDef):
        return cls._instance_cache.setdefault(type_def, cls(type_def))

    def __init__(self, type_def: TypeDef):
        self.wrappers = type_def.wrappers
        self.low_card = 'LowCardinality' in self.wrappers
        if type_def.format:
            self.format = type_def.format
        self.nullable = 'Nullable' in self.wrappers
        if self.nullable:
            self.from_row_binary = self._nullable_from_row_binary
            self.to_row_binary = self._nullable_to_row_binary
        else:
            self.to_row_binary = self._to_row_binary
            self.from_row_binary = self._from_row_binary

    @property
    def name(self):
        name = f'{self._ch_name}{self._name_suffix}'
        for wrapper in reversed(self.wrappers):
            name = f'{wrapper}({name})'
        return name

    def write_native_prefix(self, dest: MutableSequence):
        if self.low_card:
            write_uint64(low_card_version, dest)

    def read_native_prefix(self, source: Sequence, loc: int):
        if self.low_card:
            v, loc = read_uint64(source, loc)
            assert v == low_card_version
        return loc

    def read_native_column(self, source: Sequence, loc: int, num_rows: int, **kwargs):
        loc = self.read_native_prefix(source, loc)
        return self.read_native_data(source, loc, num_rows, **kwargs)

    def read_native_data(self, source: Sequence, loc: int, num_rows: int, use_none=True):
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
    def _read_native_binary(self, _source: Sequence, _loc: int, _num_rows: int) -> Tuple[
        Union[Sequence, MutableSequence], int]:
        return [], 0

    def _write_native_binary(self, column: Union[Sequence, MutableSequence], dest: MutableSequence):
        pass

    def write_native_column(self, column: Sequence, dest: MutableSequence):
        self.write_native_prefix(dest)
        self.write_native_data(column, dest)

    def write_native_data(self, column: Sequence, dest: MutableSequence):
        if self.low_card:
            self._write_native_low_card(column, dest)
        else:
            if self.nullable:
                dest += bytes([1 if x is None else 0 for x in column])
            self._write_native_binary(column, dest)

    @abstractmethod
    def _from_row_binary(self, source: Sequence, loc: int):
        pass

    @abstractmethod
    def _to_row_binary(self, value: Any, dest: MutableSequence):
        pass

    def _nullable_from_row_binary(self, source, loc, use_none: bool = True) -> (Any, int):
        if source[loc] == 0:
            return self._from_row_binary(source, loc + 1)
        return None if use_none else self.python_null, loc + 1

    def _nullable_to_row_binary(self, value, dest: bytearray):
        if value is None:
            dest += b'\x01'
        else:
            dest += b'\x00'
            self._to_row_binary(value, dest)

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
                keys = (None if use_none else self.python_null,) + keys[1:]
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
    _signed = True
    _array_type = None
    _struct_type = None
    python_type = int

    def __init_subclass__(cls, registered: bool = True):
        super().__init_subclass__(registered)
        if cls._array_type in ('i', 'I') and int_size == 2:
            cls._array_type = 'L' if cls._array_type.isupper() else 'l'
        if cls._array_type:
            cls._struct_type = '<' + cls._array_type

    def _read_native_binary(self, source: Sequence, loc: int, num_rows: int):
        return array_column(self._array_type, source, loc, num_rows)

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
    def __init__(self, type_def: TypeDef):
        super().__init__(type_def)
        self._name_suffix = type_def.arg_str

    def _from_row_binary(self, *_args):
        raise NotSupportedError(f'{self.name} deserialization not supported')

    def _to_row_binary(self, *_args):
        raise NotSupportedError(f'{self.name} serialization not supported')

    def _read_native_binary(self, source: Sequence, loc: int, num_rows: int):
        raise NotSupportedError(f'{self.name} deserialization not supported')

    def _write_native_binary(self, column: Union[Sequence, MutableSequence], dest: MutableSequence):
        raise NotSupportedError(f'{self.name} serialization  not supported')
