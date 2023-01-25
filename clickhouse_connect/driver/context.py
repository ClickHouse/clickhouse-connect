import logging
from typing import Optional, Dict, Union, Any

from clickhouse_connect.datatypes.base import ClickHouseType
from clickhouse_connect.datatypes.container import Array
from clickhouse_connect.datatypes.format import format_map

logger = logging.getLogger(__name__)


class BaseQueryContext:
    def __init__(self,
                 settings: Optional[Dict[str, Any]] = None,
                 query_formats: Optional[Dict[str, str]] = None,
                 column_formats: Optional[Dict[str, Union[str, Dict[str, str]]]] = None,
                 encoding: Optional[str] = None,
                 use_numpy: bool = False):
        self.settings = settings or {}
        self.query_formats = query_formats or {}
        self.column_formats = column_formats or {}
        self.query_overrides = format_map(query_formats)
        self.column_overrides = {}
        self.encoding = encoding
        self.use_numpy = use_numpy

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
            self.column_overrides = fmt_map
        else:
            self.column_overrides = {}
