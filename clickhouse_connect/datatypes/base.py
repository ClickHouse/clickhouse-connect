import array
from typing import NamedTuple, Callable, Dict, Type, Tuple, Any, Sequence

from clickhouse_connect.driver.common import must_swap, array_type, int_size


class TypeDef(NamedTuple):
    size: int
    wrappers: tuple
    keys: tuple
    values: tuple

    @property
    def arg_str(self):
        return f"({', '.join(str(v) for v in self.values)})"


class ClickHouseType():
    __slots__ = ('name_suffix', 'from_row_binary', 'to_row_binary', 'nullable', 'low_card', 'from_native',
                 'to_python', '__dict__')
    _instance_cache = None
    _from_row_binary = None
    _to_row_binary = None
    _to_native = None
    _to_python = None
    _from_native = None

    def __init_subclass__(cls, registry_name: str = None, registered: bool = True):
        if registered:
            cls._instance_cache: Dict[TypeDef, 'ClickHouseType'] = {}
            type_map[(registry_name or cls.__name__).upper()] = cls

    @classmethod
    def build(cls: Type['ClickHouseType'], type_def: TypeDef):
        return cls._instance_cache.setdefault(type_def, cls(type_def))

    def __init__(self, type_def: TypeDef):
        self.extra = {}
        self.name_suffix: str = ''
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
        elif self.low_card:
            self.from_native = self._low_card_from_native
        else:
            self.from_native = self._from_native

    @property
    def name(self):
        name = f'{self.__class__.__name__}{self.name_suffix}'
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

    def _nullable_from_native(self, source: Sequence, loc: int, num_rows: int):
        null_map = memoryview(source[loc: loc + num_rows])
        loc += num_rows
        column, loc = self._from_native(source, loc, num_rows)
        for ix in range(num_rows):
            if null_map[ix]:
                column[ix] = None
        return column, loc

    def _low_card_from_native(self, source: Sequence, loc: int, num_rows: int):
        loc += 8  # Skip dictionary version
        key_size = 2 ** source[loc]
        loc += 8  # Skip remaining key information
        index_cnt = int.from_bytes(source[loc: loc + 8], 'little', signed=False)
        loc += 8
        values, loc = self._from_native(source, loc, index_cnt)
        if self.nullable:
            try:
                values[0] = None
            except TypeError:
                values = (None,) + values[1:]
        loc += 8  # key size should match row count
        keys_sz = key_size * num_rows
        end = loc + keys_sz
        keys = array.array(array_type(key_size, False))
        keys.frombytes(source[loc: end])
        return tuple(values[key] for key in keys), end


class FixedType(ClickHouseType, registered=False):
    _signed = True
    _byte_size = 0
    _array_type = None

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        if not cls._array_type and cls._byte_size:
            cls._array_type = array_type(cls._byte_size, cls._signed)
        elif cls._array_type in ('i', 'I') and int_size == 2:
            cls._array_type = 'L' if cls._array_type.isupper() else 'l'

    def __init__(self, type_def: TypeDef):
        if self._array_type:
            self._from_native = self._from_array
        elif self._byte_size:
            self._from_native = self._from_bytes
        super().__init__(type_def)

    def _from_bytes(self, source: Sequence, loc: int, num_rows: int):
        sz = self._byte_size
        end = loc + sz * num_rows
        raw = [bytes(source[ix:ix + sz]) for ix in range(loc, end, sz)]
        return self._to_python(raw) if self._to_python else raw, end

    def _from_array(self, source: Sequence, loc: int, num_rows: int):
        column = array.array(self._array_type)
        sz = column.itemsize * num_rows
        column.frombytes(source[loc: loc + sz])
        loc += sz
        if must_swap:
            column.byteswap()
        if self._to_python:
            column = self._to_python(column)
        return column, loc

    def _nullable_from_native(self, source: Sequence, loc: int, num_rows: int):
        null_map = memoryview(source[loc: loc + num_rows])
        loc += num_rows
        column, loc = self._from_native(source, loc, num_rows)
        return [None if null_map[ix] else column[ix] for ix in range(num_rows)], loc


type_map: Dict[str, Type[ClickHouseType]] = {}
