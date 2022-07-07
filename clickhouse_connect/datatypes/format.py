import re

from typing import Dict, Type, Sequence

from clickhouse_connect.datatypes.base import ClickHouseType, type_map, ch_read_formats, ch_write_formats
from clickhouse_connect.driver.exceptions import ProgrammingError


def set_default_formats(*args, **kwargs):
    fmt_map = format_map(_convert_arguments(*args, **kwargs))
    ch_read_formats.update(fmt_map)
    ch_write_formats.update(fmt_map)


def clear_all_formats():
    ch_read_formats.clear()
    ch_write_formats.clear()


def clear_default_format(pattern: str):
    for ch_type in _matching_types(pattern):
        ch_read_formats.pop(ch_type, None)
        ch_write_formats.pop(ch_type, None)


def clear_write_format(pattern: str):
    for ch_type in _matching_types(pattern):
        ch_write_formats.pop(ch_type, None)


def clear_read_format(pattern: str):
    for ch_type in _matching_types(pattern):
        ch_read_formats.pop(ch_type, None)


def format_map(fmt_map: Dict[str, str]) -> Dict[Type[ClickHouseType], str]:
    if not fmt_map:
        return {}
    final_map = {}
    for pattern, fmt in fmt_map.items():
        matches = _matching_types(pattern)
        if not matches:
            raise ProgrammingError(f'Unrecognized ClickHouse type {pattern} when setting formats')
        for ch_type in matches:
            final_map[ch_type] = fmt
    return final_map


def _convert_arguments(*args, **kwargs) -> Dict[str, str]:
    fmt_map = {}
    try:
        for x in range(0, len(args), 2):
            fmt_map[args[x]] = args[x + 1]
    except (IndexError, TypeError, ValueError) as ex:
        raise ProgrammingError('Invalid type/format arguments for format method') from ex
    fmt_map.update(kwargs)
    return fmt_map


def _matching_types(pattern: str) -> Sequence[Type[ClickHouseType]]:
    if '*' in pattern:
        re_pattern = re.compile(pattern.replace('*', '.*'), re.IGNORECASE)
        return [ch_type for type_name, ch_type in type_map.items() if re_pattern.match(type_name)]
    if pattern in type_map:
        return [type_map[pattern]]
    return []
