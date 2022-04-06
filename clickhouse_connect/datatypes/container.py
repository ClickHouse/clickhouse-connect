import array
from collections.abc import Sequence, MutableSequence
from typing import Dict

from clickhouse_connect.datatypes.base import UnsupportedType, ClickHouseType, TypeDef
from clickhouse_connect.driver.common import read_leb128, to_leb128, read_uint64, array_column, low_card_version, \
    write_uint64, must_swap
from clickhouse_connect.datatypes.registry import get_from_name
from clickhouse_connect.driver import DriverError


class Array(ClickHouseType):
    __slots__ = ('element_type',)

    def __init__(self, type_def: TypeDef):
        super().__init__(type_def)
        self.element_type = get_from_name(type_def.values[0])
        if isinstance(self.element_type, Array):
            raise DriverError('Nested arrays not supported')
        self._name_suffix = f'({self.element_type.name})'

    def _from_row_binary(self, source: bytearray, loc: int):
        size, loc = read_leb128(source, loc)
        values = []
        for _ in range(size):
            value, loc = self.element_type.from_row_binary(source, loc)
            values.append(value)
        return values, loc

    def _to_row_binary(self, value: Sequence, dest: MutableSequence):
        dest += to_leb128(len(value))
        conv = self.element_type.to_row_binary
        for x in value:
            conv(x, dest)

    def _from_native(self, source: Sequence, loc: int, num_rows: int, **kwargs):
        lc_version = kwargs.pop('lc_version', None)
        if self.element_type.low_card:
            lc_version, loc = read_uint64(source, loc)
        offsets, loc = array_column('Q', source, loc, num_rows)
        if not offsets:
            return [], loc
        # pylint: disable=protected-access
        all_values, loc = self.element_type._from_native(source, loc, offsets[-1], lc_version=lc_version, **kwargs)
        column = []
        app = column.append
        last = 0
        for offset in offsets:
            app(tuple(all_values[last: offset]))
            last = offset
        return column, loc

    def _to_native(self, column: Sequence, dest: MutableSequence, lc_version=None, **_):
        if lc_version is None and self.element_type.low_card:
            lc_version = low_card_version
            write_uint64(lc_version, dest)
        offsets = array.array('Q')
        total = 0
        for x in column:
            total += len(x)
            offsets.append(total)
        if must_swap:
            offsets.byteswap()
        dest += offsets.tobytes()
        conv = self.element_type.to_native
        for x in column:
            conv(x, dest, lc_version=lc_version)


class Tuple(ClickHouseType):
    _slots = 'from_rb_funcs', 'to_rb_funcs'

    def __init__(self, type_def: TypeDef):
        super().__init__(type_def)
        element_types = [get_from_name(name) for name in type_def.values]
        self.from_rb_funcs = tuple((t.from_row_binary for t in element_types))
        self.to_rb_funcs = tuple((t.to_row_binary for t in element_types))
        self.from_native_funcs = tuple((t.from_native for t in element_types))
        self.to_native_funcs = tuple((t.to_native for t in element_types))
        self._name_suffix = type_def.arg_str

    def _from_row_binary(self, source: bytes, loc: int):
        values = []
        for conv in self.from_rb_funcs:
            value, loc = conv(source, loc)
            values.append(value)
        return tuple(values), loc

    def _to_row_binary(self, value: Sequence, dest: MutableSequence):
        for x, conv in zip(value, self.to_rb_funcs):
            conv(x, dest)

    def _from_native(self, source, loc, num_rows, **kwargs):
        columns = []
        for conv in self.from_native_funcs:
            column, loc = conv(source, loc, num_rows, **kwargs)
            columns.append(tuple(column))
        return tuple(zip(*columns)), loc

    def _to_native(self, column: Sequence, dest: MutableSequence, **kwargs):
        columns = zip(*column)
        for conv, elem_column in zip(self.to_native_funcs, columns):
            conv(elem_column, dest, **kwargs)


class Map(ClickHouseType):
    _slots = 'key_type', 'value_type', 'key_from_rb', 'key_to_rb', 'value_from_rb', 'value_to_rb'

    def __init__(self, type_def: TypeDef):
        super().__init__(type_def)
        self.key_type = get_from_name(type_def.values[0])
        self.key_from_rb, self.key_to_rb = self.key_type.from_row_binary, self.key_type.to_row_binary
        self.value_type = get_from_name(type_def.values[1])
        self.value_from_rb, self.value_to_rb = self.value_type.from_row_binary, self.value_type.to_row_binary
        self._name_suffix = type_def.arg_str

    def _from_row_binary(self, source: Sequence, loc: int):
        size, loc = read_leb128(source, loc)
        values = {}
        key_from = self.key_from_rb
        value_from = self.value_from_rb
        for _ in range(size):
            key, loc = key_from(source, loc)
            value, loc = value_from(source, loc)
            values[key] = value
        return values, loc

    def _to_row_binary(self, value: Dict, dest: bytearray):
        key_to = self.key_to_rb
        value_to = self.value_to_rb
        for k, v in value.items():
            dest += key_to(k, dest)
            dest += value_to(v, dest)

    # pylint: disable=too-many-locals
    def _from_native(self, source: Sequence, loc: int, num_rows: int, **kwargs):
        kwargs.pop('lc_version', None)
        key_version = None
        value_version = None
        if self.key_type.low_card:
            key_version, loc = read_uint64(source, loc)
        if self.value_type.low_card:
            value_version = read_uint64(source, loc)
        offsets, loc = array_column('Q', source, loc, num_rows)
        total_rows = offsets[-1]
        keys, loc = self.key_type.from_native(source, loc, total_rows, lc_version=key_version, **kwargs)
        values, loc = self.value_type.from_native(source, loc, total_rows, lc_version=value_version, **kwargs)
        all_pairs = tuple(zip(keys, values))
        column = []
        app = column.append
        last = 0
        for offset in offsets:
            app(dict(all_pairs[last: offset]))
            last = offset
        return column, loc

    def _to_native(self, column: Sequence, dest: MutableSequence, **kwargs):
        lc_version = kwargs.pop('lc_version', low_card_version)
        if self.key_type.low_card:
            write_uint64(lc_version, dest)
        if self.value_type.low_card:
            write_uint64(lc_version, dest)
        offsets = array.array('Q')
        keys = []
        values = []
        total = 0
        for v in column:
            total += len(v)
            offsets.append(total)
            keys.append(v.keys())
            values.append(v.values())
        if must_swap:
            offsets.byteswap()
        dest += offsets.tobytes()
        self.key_type.to_native(keys, dest, lc_version=lc_version)
        self.value_type.to_native(keys, dest, lc_version=lc_version)


class Object(UnsupportedType):
    def __init__(self, type_def):
        super().__init__(type_def)
        self._name_suffix = type_def.arg_str


class JSON(UnsupportedType):
    pass


class Nested(UnsupportedType):
    def __init__(self, type_def):
        super().__init__(type_def)
        self._name_suffix = type_def.arg_str
