from math import log
from typing import NamedTuple, Dict, Type, Tuple, Any, Sequence, MutableSequence, Optional

from clickhouse_connect.datatypes.common import array_column, array_type, int_size, read_uint64, write_array, \
    write_uint64, low_card_version
from clickhouse_connect.driver.exceptions import NotSupportedError


class TypeDef(NamedTuple):
    size: int
    wrappers: tuple
    keys: tuple
    values: tuple

    @property
    def arg_str(self):
        return f"({', '.join(str(v) for v in self.values)})" if self.values else ''


class ClickHouseType():
    __slots__ = ('from_row_binary', 'to_row_binary', 'nullable', 'low_card', 'from_native', 'to_native', '__dict__')
    _instance_cache = None
    _from_row_binary = None
    _to_row_binary = None
    _to_native = None
    _to_python = None
    _from_native = None
    _from_python = None
    _ch_null = None
    _name_suffix = ''

    def __init_subclass__(cls, registered: bool = True):
        if registered:
            cls._instance_cache: Dict[TypeDef, 'ClickHouseType'] = {}
            type_map[cls.__name__.upper()] = cls

    @classmethod
    def build(cls: Type['ClickHouseType'], type_def: TypeDef):
        return cls._instance_cache.setdefault(type_def, cls(type_def))

    def __init__(self, type_def: TypeDef):
        self.wrappers: Tuple[str] = type_def.wrappers
        self.low_card = 'LowCardinality' in self.wrappers
        self.nullable = 'Nullable' in self.wrappers
        if self.nullable:
            self.from_row_binary = self._nullable_from_row_binary
            self.to_row_binary = self._nullable_to_row_binary
        else:
            self.to_row_binary = self._to_row_binary
            self.from_row_binary = self._from_row_binary
        if self.nullable and not self.low_card:
            self.from_native = self._nullable_from_native
            self.to_native = self._nullable_to_native
        elif self.low_card:
            self.from_native = self._low_card_from_native
            self.to_native = self._low_card_to_native
        else:
            self.from_native = self._from_native
            self.to_native = self._to_native

    @property
    def name(self):
        name = f'{self.__class__.__name__}{self._name_suffix}'
        for wrapper in self.wrappers:
            name = f'{wrapper}({name})'
        return name

    def _nullable_from_row_binary(self, source, loc) -> (Any, int):
        if source[loc] == 0:
            return self._from_row_binary(source, loc + 1)
        return None, loc + 1

    def _nullable_to_row_binary(self, value, dest: bytearray):
        if value is None:
            dest += b'\x01'
        else:
            dest += b'\x00'
            self._to_row_binary(value, dest)

    def _nullable_from_native(self, source: Sequence, loc: int, num_rows: int, **kwargs):
        null_map = memoryview(source[loc: loc + num_rows])
        loc += num_rows
        column, loc = self._from_native(source, loc, num_rows, **kwargs)
        for ix in range(num_rows):
            if null_map[ix]:
                column[ix] = None
        return column, loc

    def _nullable_to_native(self, column: Sequence, dest: MutableSequence, **kwargs):
        dest += bytes(1 if x is None else 0 for x in column)
        column = self._replace_nulls(column)
        self._to_native(column, dest, **kwargs)

    def _replace_nulls(self, column: Sequence):
        nv = self._ch_null
        return [x if x is not None else nv for x in column]

    def _low_card_from_native(self, source: Sequence, loc: int, num_rows: int, **kwargs):
        lc_version = kwargs.pop('lc_version', None)
        if num_rows == 0:
            return tuple(), loc
        if lc_version is None:
            loc += 8  # Skip dictionary version for now
        key_size = 2 ** source[loc]  # first byte is the key size
        loc += 8  # Skip remaining key information
        index_cnt, loc = read_uint64(source, loc)
        values, loc = self._from_native(source, loc, index_cnt, **kwargs)
        if self.nullable:
            try:
                values[0] = None
            except TypeError:
                values = (None,) + values[1:]
        loc += 8  # key size should match row count
        keys, end = array_column(array_type(key_size, False), source, loc, num_rows)
        return tuple(values[key] for key in keys), end

    def _low_card_to_native(self, column: Sequence, dest: MutableSequence, lc_version=None, **kwargs):
        if lc_version is None:
            write_uint64(low_card_version, dest)
        if not column:
            return
        index = []
        keys = []
        rev_map = {}
        rmg = rev_map.get
        if self.nullable:
            index.append(0)
            keys.append(self._ch_null)
            key = 1
            for v in column:
                if v is None:
                    index.append(0)
                else:
                    ix = rmg(v)
                    if ix is None:
                        index.append(key)
                        keys.append(v)
                        rev_map[v] = key
                        key += 1
                    else:
                        index.append(ix)
        else:
            key = 0
            for v in column:
                ix = rmg(v)
                if ix is None:
                    index.append(key)
                    keys.append(v)
                    rev_map[v] = key
                    key += 1
                else:
                    index.append(ix)
        ix_type = int(log(len(keys), 2)) // 8   # power of two needed to store the total number of keys
        write_uint64((1 << 9) | (1 << 10) | ix_type, dest)  # Index type plus new dictionary (9) and additional keys(10)
        write_uint64(len(keys), dest)
        self._to_native(keys, dest, lc_version=lc_version, **kwargs)
        write_uint64(len(index), dest)
        write_array(array_type(2 ** ix_type, False), index, dest)

    def _first_value(self,  column: Sequence) -> Optional[Any]:
        if self.nullable:
            return next((x for x in column if x is not None), None)
        if column:
            return column[0]


type_map: Dict[str, Type[ClickHouseType]] = {}


class FixedType(ClickHouseType, registered=False):
    _signed = True
    _byte_size = 0
    _array_type = None
    _ch_null = 0

    @staticmethod
    def _to_zeros(column: Sequence):
        return [0 if x is None else x for x in column]

    def _to_zero_bytes(self, column:Sequence):
        empty = bytes(0 for _ in range(self._byte_size))
        return [empty if x is None else x for x in column]

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        if not cls._array_type and cls._byte_size:
            cls._array_type = array_type(cls._byte_size, cls._signed)
        elif cls._array_type in ('i', 'I') and int_size == 2:
            cls._array_type = 'L' if cls._array_type.isupper() else 'l'

    def __init__(self, type_def: TypeDef):
        if self._array_type:
            self._from_native = self._from_array
            self._to_native = self._to_array
        elif self._byte_size:
            self._from_native = self._from_bytes
            self._to_native = self._to_bytes
            self._ch_null = bytes(0 for _ in range(self._byte_size))
        super().__init__(type_def)

    def _from_bytes(self, source: Sequence, loc: int, num_rows: int, **_):
        sz = self._byte_size
        end = loc + sz * num_rows
        column = [bytes(source[ix:ix + sz]) for ix in range(loc, end, sz)]
        if self._to_python:
            column = self._to_python(column)
        return column, end

    def _to_bytes(self, column: Sequence, dest: MutableSequence, **_):
        if self._from_python:
            column = self._from_python(column)
        for x in column:
            dest += x

    def _from_array(self, source: Sequence, loc: int, num_rows: int, **_):
        column, loc = array_column(self._array_type, source, loc, num_rows)
        if self._to_python:
            column = self._to_python(column)
        return column, loc

    def _to_array(self, column: Sequence, dest: MutableSequence, **_):
        if self._from_python:
            column = self._from_python(column)
        write_array(self._array_type, column, dest)

    def _nullable_from_native(self, source: Sequence, loc: int, num_rows: int, **kwargs):
        null_map = memoryview(source[loc: loc + num_rows])
        loc += num_rows
        column, loc = self._from_native(source, loc, num_rows, **kwargs)
        return [None if null_map[ix] else column[ix] for ix in range(num_rows)], loc


class UnsupportedType(ClickHouseType, registered=False):
    def __init__(self, type_def: TypeDef):
        super().__init__(type_def)
        self._name_suffix = type_def.arg_str

    def _from_row_binary(self, *_):
        raise NotSupportedError(f'{self.name} deserialization not supported')

    def _to_row_binary(self, *_):
        raise NotSupportedError('{self.name} serialization not supported')

    def _from_native(self, *_):
        raise NotSupportedError('{self.name} deserialization not supported')

    def _to_native(self, *_):
        raise NotSupportedError('{self.name} serialization  not supported')
