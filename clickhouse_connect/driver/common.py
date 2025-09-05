import array
from enum import Enum
import struct
import sys

from typing import Sequence, MutableSequence, Dict, Optional, Union, Generator, Callable

from clickhouse_connect.driver.exceptions import ProgrammingError, StreamClosedError, DataError
from clickhouse_connect.driver.types import Closable

# pylint: disable=invalid-name
must_swap = sys.byteorder == 'big'
int_size = array.array('i').itemsize
low_card_version = 1

array_map = {1: 'b', 2: 'h', 4: 'i', 8: 'q'}
decimal_prec = {32: 9, 64: 18, 128: 38, 256: 79}

if int_size == 2:
    array_map[4] = 'l'

array_sizes = {v: k for k, v in array_map.items()}
array_sizes['f'] = 4
array_sizes['d'] = 8
np_date_types = {0: '[s]', 3: '[ms]', 6: '[us]', 9: '[ns]'}


def array_type(size: int, signed: bool):
    """
    Determines the Python array.array code for the requested byte size
    :param size: byte size
    :param signed: whether int types should be signed or unsigned
    :return: Python array.array code
    """
    try:
        code = array_map[size]
    except KeyError:
        return None
    return code if signed else code.upper()


def write_array(code: str, column: Sequence, dest: MutableSequence, col_name: Optional[str]=None):
    """
    Write a column of native Python data matching the array.array code
    :param code: Python array.array code matching the column data type
    :param column: Column of native Python values
    :param dest: Destination byte buffer
    :param col_name: Optional column name for error tracking
    """
    try:
        buff = struct.Struct(f'<{len(column)}{code}')
        dest += buff.pack(*column)
    except (TypeError, OverflowError, struct.error) as ex:
        col_msg = ''
        if col_name:
            col_msg = f' for source column `{col_name}`'
        raise DataError(f'Unable to create Python array{col_msg}.  This is usually caused by trying to insert None ' +
                                  'values into a ClickHouse column that is not Nullable') from ex


def write_uint64(value: int, dest: MutableSequence):
    """
    Write a single UInt64 value to a binary write buffer
    :param value: UInt64 value to write
    :param dest: Destination byte buffer
    """
    dest.extend(value.to_bytes(8, 'little'))


def write_leb128(value: int, dest: MutableSequence):
    """
    Write a LEB128 encoded integer to a target binary buffer
    :param value: Integer value (positive only)
    :param dest: Target buffer
    """
    while True:
        b = value & 0x7f
        value >>= 7
        if value == 0:
            dest.append(b)
            return
        dest.append(0x80 | b)


def decimal_size(prec: int):
    """
    Determine the bit size of a ClickHouse or Python Decimal needed to store a value of the requested precision
    :param prec: Precision of the Decimal in total number of base 10 digits
    :return: Required bit size
    """
    if prec < 1 or prec > 79:
        raise ArithmeticError(f'Invalid precision {prec} for ClickHouse Decimal type')
    if prec < 10:
        return 32
    if prec < 19:
        return 64
    if prec < 39:
        return 128
    return 256


def unescape_identifier(x: str) -> str:
    if x.startswith('`') and x.endswith('`'):
        return x[1:-1]
    return x


def dict_copy(source: Dict = None, update: Optional[Dict] = None) -> Dict:
    copy = source.copy() if source else {}
    if update:
        copy.update(update)
    return copy


def dict_add(source: Dict, key: str, value: any) -> Dict:
    if value is not None:
        source[key] = value
    return source


def empty_gen():
    yield from ()


def coerce_int(val: Optional[Union[str, int]]) -> int:
    if not val:
        return 0
    return int(val)


def coerce_bool(val: Optional[Union[str, bool]]):
    if not val:
        return False
    return val is True or (isinstance(val, str) and val.lower() in ('true', '1', 'y', 'yes'))


def first_value(column: Sequence, nullable:bool = True):
    if nullable:
        return next((x for x in column if x is not None), None)
    if len(column):
        return column[0]
    return None


class SliceView(Sequence):
    """
    Provides a view into a sequence rather than copying.  Borrows liberally from
    https://gist.github.com/mathieucaroff/0cf094325fb5294fb54c6a577f05a2c1
    Also see the discussion on SO: https://stackoverflow.com/questions/3485475/can-i-create-a-view-on-a-python-list
    """
    slots = ('_source', '_range')

    def __init__(self, source: Sequence, source_slice: Optional[slice] = None):
        if isinstance(source, SliceView):
            self._source = source._source
            self._range = source._range[source_slice]
        else:
            self._source = source
            if source_slice is None:
                self._range = range(len(source))
            else:
                self._range = range(len(source))[source_slice]

    def __len__(self):
        return len(self._range)

    def __getitem__(self, i):
        if isinstance(i, slice):
            return SliceView(self._source, i)
        return self._source[self._range[i]]

    def __str__(self):
        r = self._range
        return str(self._source[slice(r.start, r.stop, r.step)])

    def __repr__(self):
        r = self._range
        return f'SliceView({self._source[slice(r.start, r.stop, r.step)]})'

    def __eq__(self, other):
        if self is other:
            return True
        if len(self) != len(other):
            return False
        for v, w in zip(self, other):
            if v != w:
                return False
        return True


class StreamContext:
    """
    Wraps a generator and its "source" in a Context.  This ensures that the source will be "closed" even if the
    generator is not fully consumed or there is an exception during consumption
    """
    __slots__ = 'source', 'gen', '_in_context'

    def __init__(self, source: Closable, gen: Generator):
        self.source = source
        self.gen = gen
        self._in_context = False

    def __iter__(self):
        return self

    def __next__(self):
        if not self._in_context:
            raise ProgrammingError('Stream should be used within a context')
        return next(self.gen)

    def __enter__(self):
        if not self.gen:
            raise StreamClosedError
        self._in_context = True
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._in_context = False
        self.source.close()
        self.gen = None


class _RenameMethod(str, Enum):
    NONE = "NONE"
    REMOVE_PREFIX = "REMOVE_PREFIX"
    TO_CAMELCASE = "TO_CAMELCASE"
    TO_CAMELCASE_WITHOUT_PREFIX = "TO_CAMELCASE_WITHOUT_PREFIX"
    TO_UNDERSCORE = "TO_UNDERSCORE"
    TO_UNDERSCORE_WITHOUT_PREFIX = "TO_UNDERSCORE_WITHOUT_PREFIX"


def _to_camel(s: str) -> str:
    if not s:
        return ""
    out, up = [], False
    for ch in s:
        if ch.isspace() or ch == "_":
            up = True
        elif up:
            out.append(ch.upper())
            up = False
        else:
            out.append(ch)
    return "".join(out)


def _to_underscore(s: str) -> str:
    if not s:
        return ""
    out, prev = [], 0
    for ch in s:
        if ch.isspace():
            if prev == 0:
                out.append("_")
            prev = 1
        elif ch.isupper():
            if prev == 0:
                out.append("_")
                out.append(ch.lower())
            elif prev == 1:
                out.append(ch.lower())
            else:
                out.append(ch)
            prev = 2
        else:
            out.append(ch)
            prev = 0
    return "".join(out)[1:] if out and out[0] == "_" else "".join(out)


def _remove_prefix(s: str) -> str:
    i = s.rfind(".")
    return s[i + 1 :] if i >= 0 else s


def get_rename_method(name: Optional[str]) -> Optional[Callable[[str], str]]:
    if name is None:
        selected_method = _RenameMethod.NONE
    else:
        normalized_name = name.strip().upper()
        try:
            selected_method = _RenameMethod(normalized_name)
        except ValueError as e:
            valid_options = [member.value for member in _RenameMethod]
            raise ValueError(
                f"Invalid option '{name}'. Expected one of {valid_options}"
            ) from e

    return RENAMER_MAPPING[selected_method]


RENAMER_MAPPING: dict[_RenameMethod, Optional[Callable[[str], str]]] = {
    _RenameMethod.NONE: None,
    _RenameMethod.REMOVE_PREFIX: _remove_prefix,
    _RenameMethod.TO_CAMELCASE: _to_camel,
    _RenameMethod.TO_CAMELCASE_WITHOUT_PREFIX: lambda s: _to_camel(_remove_prefix(s)),
    _RenameMethod.TO_UNDERSCORE: _to_underscore,
    _RenameMethod.TO_UNDERSCORE_WITHOUT_PREFIX: lambda s: _to_underscore(
        _remove_prefix(s)
    ),
}
