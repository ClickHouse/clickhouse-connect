import ipaddress
import re
import uuid
import pytz

from enum import Enum
from typing import NamedTuple, Any, Tuple, Dict, Sequence, Optional, Union
from datetime import date, datetime, tzinfo

from clickhouse_connect.datatypes.string import String
from clickhouse_connect.driver.common import dict_copy
from clickhouse_connect.json_impl import any_to_json
from clickhouse_connect.common import common_settings
from clickhouse_connect.datatypes.base import ClickHouseType
from clickhouse_connect.driver.exceptions import ProgrammingError
from clickhouse_connect.driver.options import check_pandas, check_numpy, check_arrow
from clickhouse_connect.driver.context import BaseQueryContext

commands = 'CREATE|ALTER|SYSTEM|GRANT|REVOKE|CHECK|DETACH|DROP|DELETE|KILL|' +\
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
        dict_format = common_settings.get('dict_parameter_format', 'json')
        if dict_format.lower() == 'json':
            return format_str(any_to_json(value).decode())
        if dict_format.lower() == 'map':
            pairs = [format_query_value(k, server_tz) + ':' + format_query_value(v, server_tz)
                     for k, v in value.items()]
            return f"{{{', '.join(pairs)}}}"
        raise ProgrammingError("Unrecognized 'dict_parameter_format' in global settings")
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


def np_result(result: QueryResult,
              force_structured: bool = False,
              max_str_len:int = 0):
    """
    See doc string from client.query_np
    """
    np = check_numpy()
    has_nullable = any(ch_type.nullable for ch_type in result.column_types)
    has_object = any(ch_type.np_type == 'O' and (ch_type.__class__ != String or max_str_len == 0)
                     for ch_type in result.column_types)
    if has_object or (not force_structured and has_nullable):
        np_types = [np.object_ for _ in result.column_names]
        structured = False
    else:
        structured = True
        np_types = [np.dtype(ch_type.np_type) for ch_type in result.column_types]
        if max_str_len:
            str_type = np.dtype(f'U{max_str_len}')
            np_types = [str_type if x == np.object_ else x for x in np_types]
    dtypes = np.dtype(list(zip(result.column_names, np_types)))
    if structured and not result.column_oriented:
        return np.array([tuple(row) for row in result.result_set], dtype=dtypes)
    return np.rec.fromarrays(result.result_set, dtypes)


def to_pandas_df(result: QueryResult):
    """
    Convert QueryResult to a pandas dataframe
    :param result: QueryResult from driver
    :return: Two dimensional pandas dataframe from result
    """
    pd = check_pandas()
    return pd.DataFrame(dict(zip(result.column_names, result.result_set)), columns=result.column_names)


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
