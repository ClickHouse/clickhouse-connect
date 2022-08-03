import ipaddress
import re
import uuid

from enum import Enum
from typing import NamedTuple, Any, Tuple, Dict, Sequence, Optional, Union
from datetime import date, datetime
from pytz import UTC

from clickhouse_connect.datatypes.base import ClickHouseType
from clickhouse_connect.datatypes.container import Array
from clickhouse_connect.datatypes.format import format_map
from clickhouse_connect.driver.options import HAS_NUMPY, HAS_PANDAS, check_pandas, check_numpy, HAS_ARROW, check_arrow
from clickhouse_connect.driver.threads import query_settings

if HAS_PANDAS:
    import pandas as pa

if HAS_NUMPY:
    import numpy as np

if HAS_ARROW:
    import pyarrow


# pylint: disable=too-many-instance-attributes
class QueryContext:
    """
    Argument/parameter object for queries.  This context is used to set thread/query specific formats
    """

    # pylint: disable=duplicate-code
    def __init__(self,
                 query: str = None,
                 parameters: Optional[Dict[str, Any]] = None,
                 settings: Optional[Dict[str, Any]] = None,
                 query_formats: Optional[Dict[str, str]] = None,
                 column_formats: Optional[Dict[str, Union[str, Dict[str, str]]]] = None,
                 use_none: bool = True):
        """
        Initializes various configuration settings for the query context

        :param query:  Query string with Python style format value replacements
        :param parameters: Optional dictionary of substitution values
        :param settings: Optional ClickHouse settings for the query
        :param query_formats: Optional dictionary of query formats with the key of a ClickHouse type name
          (with * wildcards) and a value of valid query formats for those types.
          The value 'encoding' can be sent to change the expected encoding for this query, with a value of
          the desired encoding such as `latin-1`
        :param column_formats: Optional dictionary of column specific formats.  The key is the column name,
          The value is either the format for the data column (such as 'string' for a UUID column) or a
          second level "format" dictionary of a ClickHouse type name and a value of query formats.  This
          secondary dictionary can be used for nested column types such as Tuples or Maps
        :param column_formats: Optional dictionary
        :param use_none:
        """
        self.query = query
        self.parameters = parameters or {}
        self.settings = settings or {}
        if query_formats:
            self.encoding = query_formats.pop('encoding', None)
        else:
            self.encoding = None
        self.query_formats = format_map(query_formats)
        self.column_formats = column_formats or {}
        self.use_none = use_none
        self.thread_local = None

    def updated_copy(self,
                     query: Optional[str] = None,
                     parameters: Optional[Dict[str, Any]] = None,
                     settings: Optional[Dict[str, Any]] = None,
                     query_formats: Optional[Dict[str, str]] = None,
                     column_formats: Optional[Dict[str, Union[str, Dict[str, str]]]] = None,
                     use_none: Optional[bool] = None) -> 'QueryContext':
        """
        Creates
        :param query:
        :param parameters:
        :param settings:
        :param query_formats:
        :param column_formats:
        :param use_none:
        :return:
        """
        copy = QueryContext()
        copy.query = query or self.query
        copy.parameters = self.parameters.update(parameters or {})
        copy.settings = self.settings.update(settings or {})
        if query_formats:
            copy.encoding = self.encoding or query_formats.pop('encoding', None)
        copy.query_formats = self.query_formats.update(query_formats or {})
        copy.column_formats = self.column_formats.update(column_formats or {})
        copy.use_none = use_none if use_none is not None else self.use_none
        return copy

    def __enter__(self):
        query_settings.query_overrides = self.query_formats
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


class QueryResult:
    """
    Wrapper class for query return values and metadata
    """

    def __init__(self, result_set: Sequence[Sequence[Any]], column_names: Tuple[str, ...],
                 column_types: Tuple[ClickHouseType, ...], query_id: str = None, summary: Dict[str, Any] = None):
        self.result_set = result_set
        self.column_names = column_names
        self.column_types = column_types
        self.query_id = query_id
        self.summary = summary

    def named_results(self):
        for row in self.result_set:
            yield dict(zip(self.column_names, row))


class DataResult(NamedTuple):
    """
    Wrapper class for data return values and metadata at the lowest level
    """
    result: Sequence[Sequence[Any]]
    column_names: Tuple[str]
    column_types: Tuple[ClickHouseType]


local_tz = datetime.now().astimezone().tzinfo
BS = '\\'
must_escape = (BS, '\'')


# pylint: disable=too-many-return-statements
def format_query_value(value, server_tz=UTC):
    """
    Format Python values in a ClickHouse query
    :param value: Python object
    :param server_tz: Server timezone for adjusting datetime values
    :return: Literal string for python value
    """
    if value is None:
        return 'NULL'
    if isinstance(value, str):
        return f"'{''.join(f'{BS}{c}' if c in must_escape else c for c in value)}'"
    if isinstance(value, date):
        return f"'{value.isoformat()}'"
    if isinstance(value, datetime):
        if value.tzinfo is None and server_tz != local_tz:
            value = value.replace(tzinfo=server_tz)
        return f"'{value.strftime('%Y-%m-%d %H:%M:%S%')}'"
    if isinstance(value, list):
        return f"[{', '.join(format_query_value(x, server_tz) for x in value)}]"
    if isinstance(value, tuple):
        return f"({', '.join(format_query_value(x, server_tz) for x in value)})"
    if isinstance(value, dict):
        pairs = [format_query_value(k, server_tz) + ':' + format_query_value(v, server_tz) for k, v in value.items()]
        return f"{{{', '.join(pairs)}}}"
    if isinstance(value, Enum):
        return format_query_value(value.value, server_tz)
    if isinstance(value, (uuid.UUID, ipaddress.IPv4Address, ipaddress.IPv6Address)):
        return f"'{value}'"
    return str(value)


comment_re = re.compile(r"(\".*?\"|\'.*?\')|(/\*.*?\*/|(--\s)[^\n]*$)", re.MULTILINE | re.DOTALL)


def remove_sql_comments(sql: str) -> str:
    """
    Remove SQL comments.  This is useful to determine the type of SQL query, such as SELECT or INSERT, but we
    don't fully trust it to correctly ignore weird quoted strings, and other edge cases, so we always pass the
    original SQL to ClickHouse (which uses a full-fledged AST/ token parser)
    :param sql:  SQL query
    :return: SQL Query without SQL comments
    """
    def replacer(match):
        # if the 2nd group (capturing comments) is not None, it means we have captured a
        # non-quoted, actual comment string, so return nothing to remove the comment
        if match.group(2):
            return ''
        # Otherwise we've actually captured a quoted string, so return it
        return match.group(1)

    return comment_re.sub(replacer, sql)


def np_result(result: QueryResult) -> 'np.array':
    """
    Convert QueryResult to a numpy array
    :param result: QueryResult from driver
    :return: Two dimensional numpy array from result
    """
    check_numpy()
    np_types = [(name, ch_type.np_type) for name, ch_type in zip(result.column_names, result.column_types)]
    return np.array(result.result_set, dtype=np_types)


def to_pandas_df(result: QueryResult) -> 'pa.DataFrame':
    """
    Convert QueryResult to a pandas dataframe
    :param result: QueryResult from driver
    :return: Two dimensional pandas dataframe from result
    """
    check_pandas()
    return pa.DataFrame(np_result(result))


def from_pandas_df(df: 'pa.DataFrame'):
    """
    Wrap a pandas dataframe in a dictionary for use as insert keyword parameters
    :param df: Pandas data frame for insert
    :return: Simple dictionary to use for client insert function keywords
    """
    check_pandas()
    return {'column_names': df.columns, 'data': df.to_numpy()}


def to_arrow(content: bytes):
    check_arrow()
    reader = pyarrow.ipc.RecordBatchFileReader(content)
    return reader.read_all()
