import array
from typing import Dict, Sequence, MutableSequence, Any

from clickhouse_connect import json_impl
from clickhouse_connect.datatypes.base import ClickHouseType, TypeDef
from clickhouse_connect.driver.common import read_leb128, to_leb128, array_column, must_swap
from clickhouse_connect.datatypes.registry import get_from_name


class Array(ClickHouseType):
    __slots__ = ('element_type',)
    python_type = list

    def __init__(self, type_def: TypeDef):
        super().__init__(type_def)
        self.element_type = get_from_name(type_def.values[0])
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

    def read_native_prefix(self, source: Sequence, loc: int):
        return self.element_type.read_native_prefix(source, loc)

    # pylint: disable=too-many-locals
    def read_native_data(self, source: Sequence, loc: int, num_rows: int, use_none: bool = True):
        final_type = self.element_type
        depth = 1
        while isinstance(final_type, Array):
            depth += 1
            final_type = final_type.element_type
        level_size = num_rows
        offset_sizes = []
        for _ in range(depth):
            level_offsets, loc = array_column('Q', source, loc, level_size)
            offset_sizes.append(level_offsets)
            level_size = level_offsets[-1] if level_offsets else 0
        if level_size:
            all_values, loc = final_type.read_native_data(source, loc, level_size, use_none)
        else:
            all_values = []
        column = all_values if isinstance(all_values, list) else list(all_values)
        for offset_range in reversed(offset_sizes):
            data = []
            last = 0
            for x in offset_range:
                data.append(column[last: x])
                last = x
            column = data
        return column, loc

    def write_native_prefix(self, dest: MutableSequence):
        self.element_type.write_native_prefix(dest)

    def write_native_data(self, column: Sequence, dest: MutableSequence):
        final_type = self.element_type
        depth = 1
        while isinstance(final_type, Array):
            depth += 1
            final_type = final_type.element_type
        for _ in range(depth):
            total = 0
            data = []
            offsets = array.array('Q')
            for x in column:
                total += len(x)
                offsets.append(total)
                data.extend(x)
            if must_swap:
                offsets.byteswap()
            dest += offsets.tobytes()
            column = data
        final_type.write_native_data(column, dest)


class Tuple(ClickHouseType):
    _slots = 'element_names', 'element_types', 'from_rb_funcs', 'to_rb_funcs'
    valid_formats = 'tuple', 'json', 'native'  # native is 'tuple' for unnamed tuples, and dict for named tuples

    def __init__(self, type_def: TypeDef):
        super().__init__(type_def)
        self.element_names = type_def.keys
        self.element_types = [get_from_name(name) for name in type_def.values]
        self.from_rb_funcs = tuple((t.from_row_binary for t in self.element_types))
        self.to_rb_funcs = tuple((t.to_row_binary for t in self.element_types))
        if self.element_names:
            self._name_suffix = f"({', '.join(k + ' ' + str(v) for k, v in zip(type_def.keys, type_def.values))})"
        else:
            self._name_suffix = type_def.arg_str

    @property
    def python_type(self):
        if self.read_format() == 'tuple':
            return tuple
        if self.read_format() == 'json':
            return str
        return dict

    def _from_row_binary(self, source: bytes, loc: int):
        values = []
        for conv in self.from_rb_funcs:
            value, loc = conv(source, loc)
            values.append(value)
        return tuple(values), loc

    def _to_row_binary(self, value: Sequence, dest: MutableSequence):
        for x, conv in zip(value, self.to_rb_funcs):
            conv(x, dest)

    def read_native_prefix(self, source: Sequence, loc: int):
        for e_type in self.element_types:
            loc = e_type.read_native_prefix(source, loc)
        return loc

    def read_native_data(self, source: Sequence, loc: int, num_rows: int, use_none=True):
        columns = []
        e_names = self.element_names
        for e_type in self.element_types:
            column, loc = e_type.read_native_data(source, loc, num_rows, use_none)
            columns.append(column)
        if e_names and self.read_format() != 'tuple':
            dicts = [{} for _ in range(num_rows)]
            for ix, x in enumerate(dicts):
                for y, key in enumerate(e_names):
                    x[key] = columns[y][ix]
            if self.read_format() == 'json':
                to_json = json_impl.any_to_json
                return [to_json(x) for x in dicts], loc
            return dicts, loc
        return tuple(zip(*columns)), loc

    def write_native_prefix(self, dest: MutableSequence):
        for e_type in self.element_types:
            e_type.write_native_prefix(dest)

    def write_native_data(self, column: Sequence, dest: MutableSequence):
        columns = list(zip(*column))
        for e_type, elem_column in zip(self.element_types, columns):
            e_type.write_native_data(elem_column, dest)


class Map(ClickHouseType):
    _slots = 'key_type', 'value_type', 'key_from_rb', 'key_to_rb', 'value_from_rb', 'value_to_rb'
    python_type = dict

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

    def read_native_prefix(self, source: Sequence, loc: int):
        loc = self.key_type.read_native_prefix(source, loc)
        loc = self.value_type.read_native_prefix(source, loc)
        return loc

    # pylint: disable=too-many-locals
    def read_native_data(self, source: Sequence, loc: int, num_rows: int, use_none=True):
        offsets, loc = array_column('Q', source, loc, num_rows)
        total_rows = offsets[-1]
        keys, loc = self.key_type.read_native_data(source, loc, total_rows, use_none)
        values, loc = self.value_type.read_native_data(source, loc, total_rows, use_none)
        all_pairs = tuple(zip(keys, values))
        column = []
        app = column.append
        last = 0
        for offset in offsets:
            app(dict(all_pairs[last: offset]))
            last = offset
        return column, loc

    def write_native_prefix(self, dest: MutableSequence):
        self.key_type.write_native_prefix(dest)
        self.value_type.write_native_prefix(dest)

    def write_native_data(self, column: Sequence, dest: MutableSequence):
        offsets = array.array('Q')
        keys = []
        values = []
        total = 0
        for v in column:
            total += len(v)
            offsets.append(total)
            keys.extend(v.keys())
            values.extend(v.values())
        if must_swap:
            offsets.byteswap()
        dest += offsets.tobytes()
        self.key_type.write_native_data(keys, dest)
        self.value_type.write_native_data(values, dest)


class Nested(ClickHouseType):
    __slots__ = 'tuple_array', 'element_names', 'element_types'
    python_type = Sequence[dict]

    def __init__(self, type_def):
        super().__init__(type_def)
        self.element_names = type_def.keys
        self.tuple_array = get_from_name(f"Array(Tuple({','.join(type_def.values)}))")
        self.element_types = self.tuple_array.element_type.element_types
        cols = [f'{x[0]} {x[1].name}' for x in zip(type_def.keys, self.element_types)]
        self._name_suffix = f"({', '.join(cols)})"

    def _to_row_binary(self, value: dict, dest: MutableSequence):
        self.tuple_array.write_native_data([tuple(sub_row[key] for key in self.element_names) for sub_row in value],
                                           dest)

    def _from_row_binary(self, source: Sequence, loc: int):
        data, loc = self.tuple_array.from_row_binary(source, loc)
        return [dict(zip(self.element_names, x)) for x in data], loc

    def read_native_prefix(self, source: Sequence, loc: int):
        return self.tuple_array.read_native_prefix(source, loc)

    def read_native_data(self, source: Sequence, loc: int, num_rows: int, use_none: bool = True):
        keys = self.element_names
        data, loc = self.tuple_array.read_native_data(source, loc, num_rows, use_none)
        return [[dict(zip(keys, x)) for x in row] for row in data], loc

    def write_native_prefix(self, dest: MutableSequence):
        self.tuple_array.write_native_prefix(dest)

    def write_native_data(self, column: Sequence, dest: MutableSequence):
        keys = self.element_names
        data = [[tuple(sub_row[key] for key in keys) for sub_row in row] for row in column]
        self.tuple_array.write_native_data(data, dest)


class JSON(ClickHouseType):
    python_type = dict

    def _to_row_binary(self, value: Any, dest: MutableSequence):
        value = json_impl.any_to_json(value)
        dest += to_leb128(len(value)) + value

    def _from_row_binary(self, source: Sequence, loc: int):
        # ClickHouse will never return JSON/Object types, just tuples
        return None, 0

    def write_native_prefix(self, dest: MutableSequence):
        dest.append(0x01)

    # pylint: disable=duplicate-code
    def write_native_data(self, column: Sequence, dest: MutableSequence):
        app = dest.append
        to_json = json_impl.any_to_json
        for x in column:
            v = to_json(x)
            sz = len(v)
            while True:
                b = sz & 0x7f
                sz >>= 7
                if sz == 0:
                    app(b)
                    break
                app(0x80 | b)
            dest += v


class Object(JSON):
    python_type = dict

    def __init__(self, type_def):
        if type_def.values[0].lower() != "'json'":
            raise NotImplementedError('Only json Object type is currently supported')
        super().__init__(type_def)
        self._name_suffix = type_def.arg_str
