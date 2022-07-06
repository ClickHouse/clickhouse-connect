from abc import ABC, abstractmethod
from typing import Sequence, Dict, Union, Type

from clickhouse_connect.datatypes.base import ClickHouseType
from clickhouse_connect.datatypes.registry import matching_types
from clickhouse_connect.driver.query import DataResult


class FormatControl:

    def __init__(self,
                 default_formats: Dict[str, str] = None,
                 read_formats: Dict[str, str] = None,
                 write_formats: Dict[str, str] = None):
        default_formats = matching_types(default_formats)
        self.read_formats = default_formats.copy()
        self.read_formats.update(matching_types(read_formats))
        self.write_formats = default_formats.copy()
        self.write_formats.update(matching_types(write_formats))
        self.read_overrides = {}
        self.write_overrides = {}

    def set_read_overrides(self, read_overrides: Dict[str, str]) -> None:
        self.read_overrides = matching_types(read_overrides)

    def set_writes_overrides(self, write_overrides: Dict[str, str]) -> None:
        self.write_overrides = matching_types(write_overrides)

    def read_format(self, ch_type: Type[ClickHouseType]) -> str:
        return self.read_overrides.get(ch_type, self.read_formats.get(ch_type, 'native'))

    def write_format(self, ch_type: Type[ClickHouseType]) -> str:
        return self.write_overrides.get(ch_type, self.write_formats.get(ch_type, 'native'))

    def clear_read_overrides(self):
        self.read_overrides = {}

    def clear_write_override(self):
        self.write_overrides = {}



class QueryFormatter:
    def __init__(self,
                 type_formats: Dict[str, str] = None,
                 column_formats: Dict[str, str] = None,
                 sub_column_formats: Dict[str, Dict[str, str]] = None):
        pass


class DataTransform(ABC):

    def __init__(self, fmt_ctl: FormatControl):
        self.base_format = fmt_ctl

    def parse_response(self, source: Sequence, type_formats: Dict[column_formats:Dict[str, Union[str, Dict[str, str]]]) -> DataResult:
        pass
