import ipaddress
import logging
import re
import uuid
import pytz

from enum import Enum
from typing import Any, Tuple, Dict, Sequence, Optional, Union, Generator, Iterator
from datetime import date, datetime, tzinfo

from pytz.exceptions import UnknownTimeZoneError

from clickhouse_connect import common
from clickhouse_connect.driver.common import dict_copy, empty_gen, StreamContext
from clickhouse_connect.driver.types import Matrix, Closable
from clickhouse_connect.json_impl import any_to_json
from clickhouse_connect.driver.exceptions import StreamClosedError, ProgrammingError
from clickhouse_connect.driver.options import check_arrow, pd_has_na
from clickhouse_connect.driver.context import BaseQueryContext

logger = logging.getLogger(__name__)
commands = 'CREATE|ALTER|SYSTEM|GRANT|REVOKE|CHECK|DETACH|DROP|DELETE|KILL|' + \
           'OPTIMIZE|SET|RENAME|TRUNCATE|USE'

limit_re = re.compile(r'\s+LIMIT($|\s)', re.IGNORECASE)
select_re = re.compile(r'(^|\s)SELECT\s', re.IGNORECASE)
insert_re = re.compile(r'(^|\s)INSERT\s*INTO', re.IGNORECASE)
command_re = re.compile(r'(^\s*)(' + commands + r')\s', re.IGNORECASE)
external_bind_re = re.compile(r'{.+:.+}')


# pylint: disable=too-many-instance-attributes
class QueryContext(BaseQueryContext):
    """
    Argument/parameter object for queries.  This context is used to set thread/query specific formats
    """

    # pylint: disable=duplicate-code,too-many-arguments,too-many-locals
    def __init__(self,
                 query: str = '',
                 parameters: Optional[Dict[str, Any]] = None,
                 settings: Optional[Dict[str, Any]] = None,
                 query_formats: Optional[Dict[str, str]] = None,
                 column_formats: Optional[Dict[str, Union[str, Dict[str, str]]]] = None,
                 encoding: Optional[str] = None,
                 server_tz: tzinfo = pytz.UTC,
                 use_none: Optional[bool] = None,
                 column_oriented: Optional[bool] = None,
                 use_numpy: Optional[bool] = None,
                 max_str_len: Optional[int] = 0,
                 query_tz: Optional[Union[str, tzinfo]] = None,
                 column_tzs: Optional[Dict[str, Union[str, tzinfo]]] = None,
                 use_na_values: Optional[bool] = None,
                 as_pandas: bool = False,
                 streaming: bool = False):
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
        :param encoding: Optional string encoding for this query, such as 'latin-1'
        :param column_formats: Optional dictionary
        :param use_none: Use a Python None for ClickHouse NULL values in nullable columns.  Otherwise the default
          value of the column (such as 0 for numbers) will be returned in the result_set
        :param max_str_len Limit returned ClickHouse String values to this length, which allows a Numpy
          structured array even with ClickHouse variable length String columns.  If 0, Numpy arrays for
          String columns will always be object arrays
        :param query_tz  Either a string or a pytz tzinfo object.  (Strings will be converted to tzinfo objects).
          Values for any DateTime or DateTime64 column in the query will be converted to Python datetime.datetime
          objects with the selected timezone
        :param column_tzs A dictionary of column names to tzinfo objects (or strings that will be converted to
          tzinfo objects).  The timezone will be applied to datetime objects returned in the query
        """
        super().__init__(settings,
                         query_formats,
                         column_formats,
                         encoding,
                         use_na_values if use_na_values is not None else False,
                         use_numpy if use_numpy is not None else False)
        self.query = query
        self.parameters = parameters or {}
        self.server_tz = server_tz
        self.use_none = True if use_none is None else use_none
        self.column_oriented = False if column_oriented is None else column_oriented
        self.use_numpy = use_numpy
        self.max_str_len = 0 if max_str_len is None else max_str_len
        if query_tz is not None:
            if isinstance(query_tz, str):
                try:
                    query_tz = pytz.timezone(query_tz)
                except UnknownTimeZoneError as ex:
                    raise ProgrammingError('query_tz is not recognized') from ex
        self.query_tz = query_tz
        self.active_tz = query_tz
        if column_tzs is not None:
            for col_name, timezone in column_tzs.items():
                if isinstance(timezone, str):
                    try:
                        timezone = pytz.timezone(timezone)
                        column_tzs[col_name] = timezone
                    except UnknownTimeZoneError as ex:
                        raise ProgrammingError('query_tz is not recognized') from ex
        self.column_tzs = column_tzs
        self.block_info = False
        self.as_pandas = as_pandas
        self.use_pandas_na = as_pandas and pd_has_na
        self.streaming = streaming
        self._update_query()

    @property
    def is_select(self) -> bool:
        return select_re.search(self.uncommented_query) is not None

    @property
    def has_limit(self) -> bool:
        return limit_re.search(self.uncommented_query) is not None

    @property
    def is_insert(self) -> bool:
        return insert_re.search(self.uncommented_query) is not None

    @property
    def is_command(self) -> bool:
        return command_re.search(self.uncommented_query) is not None

    def set_parameters(self, parameters: Dict[str, Any]):
        self.parameters = parameters
        self._update_query()

    def set_parameter(self, key: str, value: Any):
        if not self.parameters:
            self.parameters = {}
        self.parameters[key] = value
        self._update_query()

    def start_column(self, name: str):
        super().start_column(name)
        if self.column_tzs and name in self.column_tzs:
            self.active_tz = self.column_tzs[name]
        else:
            self.active_tz = self.query_tz

    def updated_copy(self,
                     query: Optional[str] = None,
                     parameters: Optional[Dict[str, Any]] = None,
                     settings: Optional[Dict[str, Any]] = None,
                     query_formats: Optional[Dict[str, str]] = None,
                     column_formats: Optional[Dict[str, Union[str, Dict[str, str]]]] = None,
                     encoding: Optional[str] = None,
                     server_tz: Optional[tzinfo] = None,
                     use_none: Optional[bool] = None,
                     column_oriented: Optional[bool] = None,
                     use_numpy: Optional[bool] = None,
                     max_str_len: Optional[int] = None,
                     query_tz: Optional[Union[str, tzinfo]]= None,
                     column_tzs: Optional[Dict[str, Union[str, tzinfo]]] = None,
                     use_na_values: Optional[bool] = None,
                     as_pandas: bool = False,
                     streaming: bool = False) -> 'QueryContext':
        """
        Creates Query context copy with parameters overridden/updated as appropriate.
        """
        return QueryContext(query or self.query,
                            dict_copy(self.parameters, parameters),
                            dict_copy(self.settings, settings),
                            dict_copy(self.query_formats, query_formats),
                            dict_copy(self.column_formats, column_formats),
                            encoding if encoding else self.encoding,
                            server_tz if server_tz else self.server_tz,
                            self.use_none if use_none is None else use_none,
                            self.column_oriented if column_oriented is None else column_oriented,
                            self.use_numpy if use_numpy is None else use_numpy,
                            self.max_str_len if max_str_len is None else max_str_len,
                            self.query_tz if query_tz is None else query_tz,
                            self.column_tzs if column_tzs is None else column_tzs,
                            self.use_na_values if use_na_values is None else use_na_values,
                            as_pandas,
                            streaming)

    def _update_query(self):
        self.final_query, self.bind_params = bind_query(self.query, self.parameters, self.server_tz)
        self.uncommented_query = remove_sql_comments(self.final_query)


class QueryResult(Closable):
    """
    Wrapper class for query return values and metadata

    Note that the use of this object as a Python context is deprecated and that the Context interface will be
    removed in a future release.  Future usage of a QueryContext will be in either "closed" mode, where the
    stream has been consumed and collected into the result_set property, or in "stream" mode, where only
    the "stream" properties should be externally accessed.
    """

    # pylint: disable=too-many-arguments
    def __init__(self,
                 result_set: Matrix = None,
                 block_gen: Generator[Matrix, None, None] = None,
                 column_names: Tuple = (),
                 column_types: Tuple = (),
                 column_oriented: bool = False,
                 source: Closable = None,
                 query_id: str = None,
                 summary: Dict[str, Any] = None):
        self._result_rows = result_set
        self._result_columns = None
        self._block_gen = block_gen or empty_gen()
        self._in_context = False
        self.column_names = column_names
        self.column_types = column_types
        self.column_oriented = column_oriented
        self.source = source
        self.query_id = query_id
        self.summary = {} if summary is None else summary

    @property
    def result_set(self) -> Matrix:
        if self.column_oriented:
            return self.result_columns
        return self.result_rows

    @property
    def result_columns(self) -> Matrix:
        if self._result_columns is None:
            result = [[] for _ in range(len(self.column_names))]
            with self.column_block_stream as stream:
                for block in stream:
                    for base, added in zip(result, block):
                        base.extend(added)
            self._result_columns = result
        return self._result_columns

    @property
    def result_rows(self) -> Matrix:
        if self._result_rows is None:
            result = []
            with self.row_block_stream as stream:
                for block in stream:
                    result.extend(block)
            self._result_rows = result
        return self._result_rows

    def _column_block_stream(self):
        if self._block_gen is None:
            raise StreamClosedError
        block_stream = self._block_gen
        self._block_gen = None
        return block_stream

    def _row_block_stream(self):
        for block in self._column_block_stream():
            yield list(zip(*block))

    @property
    def column_block_stream(self) -> StreamContext:
        return StreamContext(self, self._column_block_stream())

    @property
    def row_block_stream(self):
        return StreamContext(self, self._row_block_stream())

    @property
    def rows_stream(self) -> StreamContext:
        def stream():
            for block in self._row_block_stream():
                for row in block:
                    yield row

        return StreamContext(self, stream())

    def named_results(self) -> Generator[dict, None, None]:
        for row in zip(*self.result_set) if self.column_oriented else self.result_set:
            yield dict(zip(self.column_names, row))

    @property
    def row_count(self) -> int:
        if self.column_oriented:
            return 0 if len(self.result_set) == 0 else len(self.result_set[0])
        return len(self.result_set)

    @property
    def first_item(self):
        if self.column_oriented:
            return {name: col[0] for name, col in zip(self.column_names, self.result_set)}
        return dict(zip(self.column_names, self.result_set[0]))

    @property
    def first_row(self):
        if self.column_oriented:
            return [col[0] for col in self.result_set]
        return self.result_set[0]

    def stream_column_blocks(self) -> Iterator:
        """
        .. deprecated:: 0.5.4
            Please use the column_block_stream property instead.  This method will be removed in a future release
        """
        return self._column_block_stream()

    def stream_row_blocks(self) -> Iterator:
        """
        .. deprecated:: 0.5.4
            Please use the row_block_stream property instead.  This method will be removed in a future release
        """
        return self._row_block_stream()

    def stream_rows(self) -> Iterator:
        """
        .. deprecated:: 0.5.4
           Please use the row_block_stream property instead.  This method will be removed in a future release
        """
        for block in self._row_block_stream():
            for row in block:
                yield row

    def close(self):
        if self.source:
            self.source.close()
            self.source = None
        if self._block_gen is not None:
            self._block_gen.close()
            self._block_gen = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


local_tz = datetime.now().astimezone().tzinfo
BS = '\\'
must_escape = (BS, '\'')


def quote_identifier(identifier: str):
    first_char = identifier[0]
    if first_char in ('`', '"') and identifier[-1] == first_char:
        # Identifier is already quoted, assume that it's valid
        return identifier
    return f'`{identifier}`'


def finalize_query(query: str, parameters: Optional[Union[Sequence, Dict[str, Any]]],
                   server_tz: Optional[tzinfo] = None) -> str:
    if not parameters:
        return query
    if hasattr(parameters, 'items'):
        return query % {k: format_query_value(v, server_tz) for k, v in parameters.items()}
    return query % tuple(format_query_value(v) for v in parameters)


def bind_query(query: str, parameters: Optional[Union[Sequence, Dict[str, Any]]],
               server_tz: Optional[tzinfo] = None) -> Tuple[str, Dict[str, str]]:
    if not parameters:
        return query, {}
    if external_bind_re.search(query) is None:
        return finalize_query(query, parameters, server_tz), {}
    return query, {f'param_{k}': format_bind_value(v, server_tz) for k, v in parameters.items()}


def format_str(value: str):
    return f"'{''.join(f'{BS}{c}' if c in must_escape else c for c in value)}'"


# pylint: disable=too-many-return-statements
def format_query_value(value: Any, server_tz: tzinfo = pytz.UTC):
    """
    Format Python values in a ClickHouse query
    :param value: Python object
    :param server_tz: Server timezone for adjusting datetime values
    :return: Literal string for python value
    """
    if value is None:
        return 'NULL'
    if isinstance(value, str):
        return format_str(value)
    if isinstance(value, datetime):
        if value.tzinfo is None and server_tz != local_tz:
            value = value.replace(tzinfo=server_tz)
        return f"'{value.strftime('%Y-%m-%d %H:%M:%S')}'"
    if isinstance(value, date):
        return f"'{value.isoformat()}'"
    if isinstance(value, list):
        return f"[{', '.join(format_query_value(x, server_tz) for x in value)}]"
    if isinstance(value, tuple):
        return f"({', '.join(format_query_value(x, server_tz) for x in value)})"
    if isinstance(value, dict):
        if common.get_setting('dict_parameter_format') == 'json':
            return format_str(any_to_json(value).decode())
        pairs = [format_query_value(k, server_tz) + ':' + format_query_value(v, server_tz)
                 for k, v in value.items()]
        return f"{{{', '.join(pairs)}}}"
    if isinstance(value, Enum):
        return format_query_value(value.value, server_tz)
    if isinstance(value, (uuid.UUID, ipaddress.IPv4Address, ipaddress.IPv6Address)):
        return f"'{value}'"
    return str(value)


def format_bind_value(value: Any, server_tz: tzinfo = pytz.UTC):
    """
    Format Python values in a ClickHouse query
    :param value: Python object
    :param server_tz: Server timezone for adjusting datetime values
    :return: Literal string for python value
    """
    if value is None:
        return 'NULL'
    if isinstance(value, datetime):
        if value.tzinfo is None and server_tz != local_tz:
            value = value.replace(tzinfo=server_tz)
        return value.strftime('%Y-%m-%d %H:%M:%S')
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, list):
        return f"[{', '.join(format_bind_value(x, server_tz) for x in value)}]"
    if isinstance(value, tuple):
        return f"({', '.join(format_bind_value(x, server_tz) for x in value)})"
    if isinstance(value, dict):
        if common.get_setting('dict_parameter_format') == 'json':
            return any_to_json(value).decode()
        pairs = [format_bind_value(k, server_tz) + ':' + format_bind_value(v, server_tz)
                 for k, v in value.items()]
        return f"{{{', '.join(pairs)}}}"
    if isinstance(value, Enum):
        return format_bind_value(value.value, server_tz)
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


def to_arrow(content: bytes):
    pyarrow = check_arrow()
    reader = pyarrow.ipc.RecordBatchFileReader(content)
    return reader.read_all()


def arrow_buffer(table) -> Tuple[Sequence[str], bytes]:
    pyarrow = check_arrow()
    sink = pyarrow.BufferOutputStream()
    with pyarrow.RecordBatchFileWriter(sink, table.schema) as writer:
        writer.write(table)
    return table.schema.names, sink.getvalue()
