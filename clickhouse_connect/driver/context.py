import logging
from typing import Optional, Dict, Union, Any

from clickhouse_connect.datatypes.base import ClickHouseType
from clickhouse_connect.datatypes.container import Array
from clickhouse_connect.datatypes.format import format_map
from clickhouse_connect.driver.threads import query_settings

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
        self.encoding = encoding
        self.use_numpy = use_numpy
        self._open = False

    def enter(self):
        old_context = getattr(query_settings, 'context', None)
        if old_context:
            logger.error('Entering new Query Context before previous context exited')
            old_context.exit()
        self._open = True
        query_settings.context = self
        query_settings.query_overrides = format_map(self.query_formats)
        query_settings.query_encoding = self.encoding

    def exit(self):
        if self._open:
            del query_settings.context
            del query_settings.query_overrides
            del query_settings.query_encoding
            self._open = False

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
