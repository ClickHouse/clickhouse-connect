import array
import re
import logging

from typing import Tuple, NamedTuple, Any, Type, Dict, List, Union

from clickhouse_connect.driver import DriverError


class TypeDef(NamedTuple):
    size: int
    wrappers: tuple
    keys: tuple
    values: tuple

    @property
    def arg_str(self):
        return f"({', '.join(str(v) for v in self.values)})"


class ClickHouseType():
    __slots__ = 'wrappers', 'from_row_binary', 'to_row_binary', 'name_suffix', 'nullable', 'output', 'extra'
    _instance_cache = None
    _from_row_binary = None
    _to_row_binary = None

    from_native = None
    to_python = None

    def __init_subclass__(cls):
        cls._instance_cache: Dict[TypeDef, 'ClickHouseType'] = {}
        type_map[cls.__name__.upper()] = cls

    @classmethod
    def build(cls: Type['ClickHouseType'], type_def: TypeDef):
        return cls._instance_cache.setdefault(type_def, cls(type_def))

    def __init__(self, type_def: TypeDef):
        self.extra = {}
        self.name_suffix:str = ''
        self.wrappers: Tuple[str] = type_def.wrappers
        if 'Nullable' in self.wrappers:
            self.from_row_binary = self._nullable_from_row_binary
            self.to_row_binary = self._nullable_to_row_binary
            self.nullable = True
        else:
            self.to_row_binary = self._to_row_binary
            self.from_row_binary = self._from_row_binary
            self.nullable = False

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


type_map: Dict[str, Type[ClickHouseType]] = {}
size_pattern = re.compile(r'^([A-Z]+)(\d+)')
int_pattern = re.compile(r'^-?\d+$')

int_size = array.array('i').itemsize


class FixedType(ClickHouseType):
    _array_type:str = ''

    def __init_subclass__(cls):
        super().__init_subclass__()
        if int_size == 2 and cls._array_type in ('i', 'I'):
            cls._array_type = 'L' if cls._array_type.isupper() else 'l'

    @classmethod
    def from_native(cls, source: Union[bytes, bytearray, memoryview], loc: int, num_rows: int, must_swap: bool):
        column = array.array(cls._array_type)
        sz = column.itemsize * num_rows
        column.frombytes(source[loc: loc + sz])
        loc += sz
        if must_swap:
            column.byteswap()
        return column, loc


def get_from_name(name: str) -> ClickHouseType:
    base = name
    size = 0
    wrappers = []
    keys = tuple()
    values = tuple()
    if base.upper().startswith('NULLABLE'):
        wrappers.append('Nullable')
        base = base[9:-1]
    if base.upper().startswith('LOWCARDINALITY'):
        wrappers.append('LowCardinality')
        base = base[15:-1]
    if base.upper().startswith('ENUM'):
        keys, values = _parse_enum(base)
        base = base[:base.find('(')]
    paren = base.find('(')
    if paren != -1:
        arg_str = base[paren + 1:-1]
        base = base[:paren]
        values = _parse_args(arg_str)
    base = base.upper()
    if base not in type_map:
        match = size_pattern.match(base)
        if match:
            base = match.group(1)
            size = int(match.group(2))
    try:
        type_cls = type_map[base]
    except KeyError:
        err_str = f'Unrecognized ClickHouse type base: {base} name: {name}'
        logging.error(err_str)
        raise DriverError(err_str)
    return type_cls.build(TypeDef(size, tuple(wrappers), keys, values))


def _parse_enum(name) -> Tuple[Tuple[str], Tuple[int]]:
    keys = []
    values = []
    pos = name.find('(') + 1
    escaped = False
    in_key = False
    key = ''
    value = ''
    while True:
        char = name[pos]
        pos += 1
        if in_key:
            if escaped:
                key += char
                escaped = False
            else:
                if char == "'":
                    keys.append(key)
                    key = ''
                    in_key = False
                elif char == '\\':
                    escaped = True
                else:
                    key += char
        elif char not in (' ', '='):
            if char == ',':
                values.append(int(value))
                value = ''
            elif char == ')':
                values.append(int(value))
                break
            elif char == "'" and not value:
                in_key = True
            else:
                value += char
    return tuple(keys), tuple(values)


def _parse_args(name) -> [Any]:
    values: List[Any] = []
    value = ''
    l = len(name)
    in_str = False
    escaped = False
    pos = 0

    def add_value():
        if int_pattern.match(value):
            values.append(int(value))
        else:
            values.append(value)

    while pos < l:
        char = name[pos]
        pos += 1
        if in_str:
            value += char
            if escaped:
                escaped = False
            else:
                if char == "'":
                    in_str = False
                elif char == '\\':
                    escaped = True
        else:
            while char == ' ':
                char = name[pos]
                pos += 1
                if pos == l:
                    break
            if char == ',':
                add_value()
                value = ''
            else:
                if char == "'" and not value:
                    in_str = True
                value += char
    if value != '':
        add_value()
    return tuple(values)
