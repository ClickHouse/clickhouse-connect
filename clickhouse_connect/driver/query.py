import ipaddress
import re
import uuid
import pytz

from enum import Enum
from typing import NamedTuple, Any, Tuple, Dict, Sequence, Optional, Union
from datetime import date, datetime, tzinfo

from clickhouse_connect import common
from clickhouse_connect.driver.common import dict_copy
from clickhouse_connect.json_impl import any_to_json
from clickhouse_connect.datatypes.base import ClickHouseType
from clickhouse_connect.driver.exceptions import ProgrammingError
from clickhouse_connect.driver.options import check_pandas, check_numpy, check_arrow
from clickhouse_connect.driver.context import BaseQueryContext

commands = 'CREATE|ALTER|SYSTEM|GRANT|REVOKE|CHECK|DETACH|DROP|DELETE|KILL|' + \
           'OPTIMIZE|SET|RENAME|TRUNCATE|USE'

limit_re = re.compile(r'\s+LIMIT($|\s)', re.IGNORECASE)
select_re = re.compile(r'(^|\s)SELECT\s', re.IGNORECASE)
insert_re = re.compile(r'(^|\s)INSERT\s*INTO', re.IGNORECASE)
command_re = re.compile(r'(^\s*)(' + commands + r')\s', re.IGNORECASE)


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
                 use_none: bool = True,
                 column_oriented: bool = False):
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
        self.use_none = use_none
        self.column_oriented = column_oriented
        self.final_query = finalize_query(query, parameters, server_tz)
        self._uncommented_query = None

    @property
    def uncommented_query(self) -> str:
        if not self._uncommented_query:
            self._uncommented_query = remove_sql_comments(self.final_query)
        return self._uncommented_query

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
        Creates Query context copy with parameters overridden/updated as appropriate
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


class QueryResult:
    """
    Wrapper class for query return values and metadata
    """

    def __init__(self,
                 result_set: Sequence[Sequence[Any]],
                 column_names: Tuple[str, ...],
                 column_types: Tuple[ClickHouseType, ...],
                 query_id: str = None,
                 summary: Dict[str, Any] = None,
                 column_oriented: bool = False):
        self.result_set = result_set
        self.column_names = column_names
        self.column_types = column_types
        self.query_id = query_id
        self.summary = summary
        self.column_oriented = column_oriented

    @property
    def empty(self):
        if self.column_oriented:
            return len(self.result_set) == 0 or len(self.result_set[0]) == 0
        return len(self.result_set) == 0

    def named_results(self):
        if self.column_oriented:
            for row in zip(*self.result_set):
                yield dict(zip(self.column_names, row))
        else:
            for row in self.result_set:
                yield dict(zip(self.column_names, row))


class DataResult(NamedTuple):
    """
    Wrapper class for data return values and metadata at the lowest level
    """
    result: Sequence[Sequence[Any]]
    column_names: Tuple[str]
    column_types: Tuple[ClickHouseType]
    column_oriented: bool = False


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
