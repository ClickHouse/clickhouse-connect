from typing import Optional, Dict, Union, Any

from clickhouse_connect.datatypes.base import ClickHouseType
from clickhouse_connect.datatypes.container import Array
from clickhouse_connect.datatypes.format import format_map
from clickhouse_connect.driver.threads import query_settings


class BaseQueryContext:
    def __init__(self,
                 settings: Optional[Dict[str, Any]] = None,
                 query_formats: Optional[Dict[str, str]] = None,
                 column_formats: Optional[Dict[str, Union[str, Dict[str, str]]]] = None,
                 encoding: Optional[str] = None):
        self.settings = settings or {}
        self.query_formats = query_formats or {}
        self.column_formats = column_formats or {}
        self.encoding = encoding

    def __enter__(self):
        query_settings.query_overrides = format_map(self.query_formats)
        query_settings.query_encoding = self.encoding
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        query_settings.query_overrides = None
        query_settings.column_overrides = None
        query_settings.query_encoding = None

    def start_column(self, name: str, ch_type: ClickHouseType):
        if name in self.column_formats:
            fmts = self.column_formats[name]
            if isinstance(fmts, str):
                if isinstance(ch_type, Array):
                    fmt_map = {ch_type.element_type.__class__: fmts}
                else:
                    fmt_map = {ch_type.__class__: fmts}
            else:
                fmt_map = format_map(fmts)
            query_settings.column_overrides = fmt_map
        else:
            query_settings.column_overrides = None
