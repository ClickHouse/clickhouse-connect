import ipaddress
import logging
import re
import uuid
import pytz

from enum import Enum
from typing import Any, Tuple, Dict, Sequence, Optional, Union, Generator, Iterator
from datetime import date, datetime, tzinfo

from clickhouse_connect import common
from clickhouse_connect.driver.common import dict_copy
from clickhouse_connect.driver.types import Matrix, Closable
from clickhouse_connect.json_impl import any_to_json
from clickhouse_connect.datatypes.base import ClickHouseType
from clickhouse_connect.driver.exceptions import ProgrammingError
from clickhouse_connect.driver.options import check_pandas, check_numpy, check_arrow
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

    # pylint: disable=duplicate-code,too-many-arguments
    def __init__(self,
                 query: str = '',
                 parameters: Optional[Dict[str, Any]] = None,
                 settings: Optional[Dict[str, Any]] = None,
                 query_formats: Optional[Dict[str, str]] = None,
                 column_formats: Optional[Dict[str, Union[str, Dict[str, str]]]] = None,
                 encoding: Optional[str] = None,
                 server_tz: tzinfo = pytz.UTC,
                 use_none: Optional[bool] = None,
                 column_oriented: Optional[bool] = None):
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
        :param use_none:
        """
        super().__init__(settings, query_formats, column_formats, encoding)
        self.query = query
        self.parameters = parameters or {}
        self.server_tz = server_tz
        self.use_none = True if use_none is None else use_none
        self.column_oriented = False if column_oriented is None else column_oriented
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

    def updated_copy(self,
                     query: Optional[str] = None,
                     parameters: Optional[Dict[str, Any]] = None,
                     settings: Optional[Dict[str, Any]] = None,
                     query_formats: Optional[Dict[str, str]] = None,
                     column_formats: Optional[Dict[str, Union[str, Dict[str, str]]]] = None,
                     encoding: Optional[str] = None,
                     server_tz: Optional[tzinfo] = None,
                     use_none: Optional[bool] = None,
                     column_oriented: Optional[bool] = None) -> 'QueryContext':
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
                            self.column_oriented if column_oriented is None else column_oriented)

    def _update_query(self):
        self.final_query, self.bind_params = bind_query(self.query, self.parameters, self.server_tz)
        self.uncommented_query = remove_sql_comments(self.final_query)


class QueryResult:
    """
    Wrapper class for query return values and metadata
    """

    # pylint: disable=too-many-arguments
    def __init__(self,
                 result_set: Matrix = None,
                 block_gen: Generator[Matrix, None, None] = None,
                 column_names: Tuple[str, ...] = (),
                 column_types: Tuple[ClickHouseType, ...] = (),
                 column_oriented: bool = False,
                 source: Closable = None,
                 query_id: str = None,
                 summary: Dict[str, Any] = None):
        self._result_rows = result_set
        self._result_columns = None
        self._block_gen = block_gen
        self._in_context = False
        self.column_names = column_names
        self.column_types = column_types
        self.column_oriented = column_oriented
        self.source = source
        self.query_id = query_id
        self.summary = {} if summary is None else summary

    @property
    def empty(self):
        return self.row_count == 0

    @property
    def result_set(self) -> Matrix:
        if self.column_oriented:
            return self.result_columns
        return self.result_rows

    @property
    def result_columns(self) -> Matrix:
        if self._result_columns is None:
            if self._result_rows is not None:
                raise ProgrammingError(
                    'result_columns referenced after result_rows.  Only one final format is supported'
                )
            result = [[] for _ in range(len(self.column_names))]
            for block in self._block_gen:
                for base, added in zip(result, block):
                    base.extend(added)
            self._result_columns = result
            self._block_gen = None
        return self._result_columns

    @property
    def result_rows(self) -> Matrix:
        if self._result_rows is None:
            if self._result_columns is not None:
                raise ProgrammingError(
                    'result_rows referenced after result_columns.  Only one final format is supported'
                )
            result = []
            for block in self._block_gen:
                result.extend(list(zip(*block)))
            self._result_rows = result
            self._block_gen = None
        return self._result_rows

    def stream_column_blocks(self) -> Iterator[Matrix]:
        if not self._in_context:
            logger.warning("Streaming results should be used in a 'with' context to ensure the stream is closed")
        if not self._block_gen:
            raise ProgrammingError('Stream closed')
        temp = self._block_gen
        self._block_gen = None
        return temp

    def stream_row_blocks(self):
        return (list(zip(*block)) for block in self.stream_column_blocks())

    def stream_rows(self) -> Iterator[Sequence]:
        for block in self.stream_column_blocks():
            for row in list(zip(*block)):
                yield row

    def named_results(self) -> Generator[dict, None, None]:
        for row in zip(*self.result_columns):
            yield dict(zip(self.column_names, row))

    @property
    def row_count(self) -> int:
        if self.column_oriented:
            return 0 if len(self.result_set) == 0 else len(self.result_set[0])
        return len(self.result_set)

    @property
    def first_item(self):
        if self.empty:
            return None
        if self.column_oriented:
            return {name: col[0] for name, col in zip(self.column_names, self.result_set)}
        return dict(zip(self.column_names, self.result_set[0]))

    @property
    def first_row(self):
        if self.empty:
            return None
        if self.column_oriented:
            return [col[0] for col in self.result_set]
        return self.result_set[0]

    def close(self):
        if self.source:
            self.source.close()
            self.source = None

    def __enter__(self):
        self._in_context = True

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        self._in_context = False


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


def np_result(result: QueryResult, use_none: bool = False, max_str_len: int = 0):
    """
    See doc string from client.query_np
    """
    np = check_numpy()
    if result.empty:
        return np.empty(0)
    if not result.column_oriented:
        raise ProgrammingError('Numpy arrays should only be constructed from column oriented query results')
    np_types = [col_type.np_type(max_str_len) for col_type in result.column_types]
    first_type = np.dtype(np_types[0])
    if first_type != np.object_ and all(np.dtype(np_type) == first_type for np_type in np_types):
        # Optimize the underlying "matrix" array without any additional processing
        return np.array(result.result_set, first_type).transpose()
    columns = []
    has_objects = False
    for column, col_type, np_type in zip(result.result_set, result.column_types, np_types):
        if np_type == 'O':
            columns.append(column)
            has_objects = True
        elif use_none and col_type.nullable:
            new_col = []
            item_array = np.empty(1, dtype=np_type)
            for x in column:
                if x is None:
                    new_col.append(None)
                    has_objects = True
                else:
                    item_array[0] = x
                    new_col.append(item_array[0])
            columns.append(new_col)
        elif 'date' in np_type:
            columns.append(np.array(column, dtype=np_type))
        else:
            columns.append(column)
    if has_objects:
        np_types = [np.object_] * len(result.column_names)
    dtypes = np.dtype(list(zip(result.column_names, np_types)))
    return np.rec.fromarrays(columns, dtypes)


def pandas_result(result: QueryResult):
    """
    Convert QueryResult to a pandas dataframe
    :param result: QueryResult from driver
    :return: Two dimensional pandas dataframe from result
    """
    pd = check_pandas()
    np = check_numpy()
    if not result.column_oriented:
        raise ProgrammingError('Pandas dataframes should only be constructed from column oriented query results')
    raw = {}
    for name, col_type, column in zip(result.column_names, result.column_types, result.result_set):
        np_type = col_type.np_type()
        if 'datetime' in np_type:
            column = pd.to_datetime(np.array(column, dtype=np_type))
        raw[name] = column
    return pd.DataFrame(raw)


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
