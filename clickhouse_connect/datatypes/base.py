from math import log
from typing import NamedTuple, Dict, Type, Any, Sequence, MutableSequence, Optional, Union

from clickhouse_connect.driver.common import array_column, array_type, int_size, read_uint64, write_array, \
    write_uint64, low_card_version, array_sizes
from clickhouse_connect.driver.exceptions import NotSupportedError


class TypeDef(NamedTuple):
    size: int = 0
    wrappers: tuple = ()
    keys: tuple = ()
    values: tuple = ()

    @property
    def arg_str(self):
        return f"({', '.join(str(v) for v in self.values)})" if self.values else ''


class ClickHouseType:
    __slots__ = 'nullable', 'low_card', 'wrappers', '__dict__'
    _instance_cache = None
    _ch_name = None
    _name_suffix = ''
    np_type = 'O'
    python_null = 0

    def __init_subclass__(cls, registered: bool = True):
        if registered:
            cls._ch_name = cls.__name__
            cls._instance_cache: Dict[TypeDef, 'ClickHouseType'] = {}
            type_map[cls._ch_name.upper()] = cls

    @classmethod
    def build(cls: Type['ClickHouseType'], type_def: TypeDef):
        return cls._instance_cache.setdefault(type_def, cls(type_def))

    def __init__(self, type_def: TypeDef):
        self.wrappers = type_def.wrappers
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
        name = f'{self._ch_name}{self._name_suffix}'
        for wrapper in reversed(self.wrappers):
            name = f'{wrapper}({name})'
        return name

    @property
    def ch_null(self):
        return b'\x00'

    def _from_row_binary(self, source: Sequence, loc: int):
        raise NotImplementedError

    def _to_row_binary(self, value: Any, dest: MutableSequence):
        raise NotImplementedError

    def _from_native(self, source: Sequence, loc: int, num_rows: int, **kwargs):
        raise NotImplementedError

    def _to_native(self, column: Sequence, dest: MutableSequence, **kwargs):
        raise NotImplementedError

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

    def _nullable_from_native(self, source: Sequence, loc: int, num_rows: int, use_none=True, **kwargs):
        null_map = memoryview(source[loc: loc + num_rows])
        loc += num_rows
        column, loc = self._from_native(source, loc, num_rows, **kwargs)
        if use_none:
            for ix in range(num_rows):
                if null_map[ix]:
                    column[ix] = None
        return column, loc

    def _nullable_to_native(self, column: Sequence, dest: MutableSequence, **kwargs):
        dest += bytes(1 if x is None else 0 for x in column)
        self._to_native(column, dest, **kwargs)

    def _low_card_from_native(self, source: Sequence, loc: int, num_rows: int, use_none=True, **kwargs):
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
                values[0] = None if use_none else self.python_null
            except TypeError:
                values = (None if use_none else self.python_null,) + values[1:]
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
            keys.append(self.ch_null)
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
        ix_type = int(log(len(keys), 2)) // 8  # power of two needed to store the total number of keys
        write_uint64((1 << 9) | (1 << 10) | ix_type, dest)  # Index type plus new dictionary (9) and additional keys(10)
        write_uint64(len(keys), dest)
        self._to_native(keys, dest, lc_version=lc_version, **kwargs)
        write_uint64(len(index), dest)
        write_array(array_type(2 ** ix_type, False), index, dest)

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


class ArrayType(ClickHouseType, registered=False):
    _signed = True
    _array_type = None
    _ch_null = None

    def __init_subclass__(cls, registered: bool = True):
        super().__init_subclass__(registered)
        if cls._array_type in ('i', 'I') and int_size == 2:
            cls._array_type = 'L' if cls._array_type.isupper() else 'l'
        if cls._array_type:
            cls._ch_null = bytes(b'\x00' * array_sizes[cls._array_type.lower()])

    @property
    def ch_null(self):
        return self._ch_null

    def _from_row_binary(self, source: bytearray, loc: int):
        raise NotImplementedError

    def _to_row_binary(self, value: Any, dest: bytearray):
        raise NotImplementedError

    def _from_native(self, source: Sequence, loc: int, num_rows: int, **_):
        column, loc = array_column(self._array_type, source, loc, num_rows)
        return column, loc

    def _to_native(self, column: Sequence, dest: MutableSequence, **_):
        write_array(self._array_type, column, dest)

    def _nullable_from_native(self, source: Sequence, loc: int, num_rows: int, use_none: bool = True, **kwargs):
        null_map = memoryview(source[loc: loc + num_rows])
        loc += num_rows
        column, loc = self._from_native(source, loc, num_rows, **kwargs)
        if use_none:
            return [None if null_map[ix] else column[ix] for ix in range(num_rows)], loc
        return column, loc

    def _nullable_to_native(self, column: Union[Sequence, MutableSequence], dest: MutableSequence, **kwargs):
        write_array('B', [1 if x is None else 0 for x in column], dest)
        first = column[0]
        try:
            column[0] = None
            for ix, x in enumerate(column):
                if not x:
                    column[ix] = 0
            column[0] = first or 0
        except TypeError:
            column = [0 if x is None else x for x in column]
        self._to_native(column, dest, **kwargs)


class UnsupportedType(ClickHouseType, registered=False):
    def __init__(self, type_def: TypeDef):
        super().__init__(type_def)
        self._name_suffix = type_def.arg_str

    def _from_row_binary(self, *_args):
        raise NotSupportedError(f'{self.name} deserialization not supported')

    def _to_row_binary(self, *_args):
        raise NotSupportedError(f'{self.name} serialization not supported')

    def _from_native(self, *_args, **_kwargs):
        raise NotSupportedError(f'{self.name} deserialization not supported')

    def _to_native(self, *_args, **_kwargs):
        raise NotSupportedError(f'{self.name} serialization  not supported')
