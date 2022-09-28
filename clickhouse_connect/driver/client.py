import logging
import pytz

from abc import ABCMeta, abstractmethod
from typing import Iterable, Tuple, Optional, Any, Union, Sequence, Dict
from pytz.exceptions import UnknownTimeZoneError

from clickhouse_connect.datatypes.registry import get_from_name
from clickhouse_connect.datatypes.base import ClickHouseType
from clickhouse_connect.driver.exceptions import ProgrammingError, InternalError
from clickhouse_connect.driver.models import ColumnDef, SettingDef
from clickhouse_connect.driver.query import QueryResult, np_result, to_pandas_df, from_pandas_df, to_arrow, \
    QueryContext, arrow_buffer

logger = logging.getLogger(__name__)


class Client(metaclass=ABCMeta):
    """
    Base ClickHouse Connect client
    """
    column_inserts = False
    valid_transport_settings = set()

    def __init__(self, database: str, query_limit: int, uri: str):
        """
        Shared initialization of ClickHouse Connect client
        :param database: database name
        :param query_limit: default LIMIT for queries
        :param uri: uri for error messages
        """
        self.limit = query_limit
        self.server_tz = pytz.UTC
        self.server_version, server_tz, self.database = \
            tuple(self.command('SELECT version(), timezone(), database()', use_database=False))
        try:
            self.server_tz = pytz.timezone(server_tz)
        except UnknownTimeZoneError:
            logger.warning('Warning, server is using an unrecognized timezone %s, will use UTC default', server_tz)
        server_settings = self.query('SELECT name, value, changed, description, type, readonly FROM system.settings')
        self.server_settings = {row['name']: SettingDef(**row) for row in server_settings.named_results()}
        if database and not database == '__default__':
            self.database = database
        self.uri = uri

    def _validate_settings(self, settings: Optional[Dict[str, Any]], stringify: bool = False) -> Dict[str, Any]:
        """
        This strips any ClickHouse settings that are not recognized or are read only
        :param settings:  Dictionary of setting name and values
        :param stringify:  Return the result dictionary values as strings
        :return:
        """
        validated = {}
        if settings:
            for key, value in settings.items():
                if key not in self.valid_transport_settings:
                    setting_def = self.server_settings.get(key)
                    if setting_def is None or setting_def.readonly:
                        logger.debug('Setting %s is not valid or read only, ignoring', key)
                        continue
                if stringify:
                    if isinstance(value, bool):
                        value = '1' if value else '0'
                    else:
                        value = str(value)
                validated[key] = value
        return validated

    def _prep_query(self, context: QueryContext):
        if context.is_select and not context.has_limit and self.limit:
            return f'{context.final_query}\n LIMIT {self.limit}'
        return context.final_query

    @abstractmethod
    def _query_with_context(self, context: QueryContext):
        pass

    @abstractmethod
    def client_setting(self, name, value):
        """
        Set a clickhouse setting for the client after initialization
        :param name: Setting name
        :param value: Setting value
        """

    # pylint: disable=duplicate-code,too-many-arguments
    def query(self,
              query: str = None,
              parameters: Optional[Union[Sequence, Dict[str, Any]]] = None,
              settings: Optional[Dict[str, Any]] = None,
              query_formats: Optional[Dict[str, str]] = None,
              column_formats: Optional[Dict[str, Union[str, Dict[str, str]]]] = None,
              encoding: Optional[str] = None,
              use_none: bool = True,
              context: QueryContext = None) -> QueryResult:
        """
        Main query method for SELECT, DESCRIBE and other SQL statements that return a result matrix
        :param query: Query statement/format string
        :param parameters: Optional dictionary used to format the query
        :param settings: Optional dictionary of ClickHouse settings (key/string values)
        :param query_formats: See QueryContext __init__ docstring
        :param column_formats: See QueryContext __init__ docstring
        :param encoding: See QueryContext __init__ docstring
        :param use_none: Use None for ClickHouse nulls instead of empty values
        :param context An alternative QueryContext parameter object that contains some or all of the method arguments
        :return: QueryResult -- data and metadata from response
        """
        if context:
            query_context = context.updated_copy(query,
                                                 parameters,
                                                 settings,
                                                 query_formats,
                                                 column_formats,
                                                 encoding,
                                                 self.server_tz,
                                                 False)
        else:
            query_context = QueryContext(query,
                                         parameters,
                                         settings,
                                         query_formats,
                                         column_formats,
                                         encoding,
                                         self.server_tz,
                                         use_none)
        return self._query_with_context(query_context)

    @abstractmethod
    def raw_query(self,
                  query: str,
                  parameters: Optional[Union[Sequence, Dict[str, Any]]] = None,
                  settings: Optional[Dict[str, Any]] = None,
                  fmt: str = None) -> bytes:
        """
        Query method that simply returns the raw ClickHouse format bytes
        :param query: Query statement/format string
        :param parameters: Optional dictionary used to format the query
        :param settings: Optional dictionary of ClickHouse settings (key/string values)
        :param fmt: ClickHouse output format
        :return: bytes representing raw ClickHouse return value based on format
        """

    # pylint: disable=duplicate-code
    def query_np(self,
                 query: str = None,
                 parameters: Optional[Union[Sequence, Dict[str, Any]]] = None,
                 settings: Optional[Dict[str, Any]] = None,
                 query_formats: Optional[Dict[str, str]] = None,
                 column_formats: Optional[Dict[str, str]] = None,
                 encoding: Optional[str] = None,
                 context: QueryContext = None):
        """
        Query method that returns the results as a numpy array
        :param query: Query statement/format string
        :param parameters: Optional dictionary used to format the query
        :param settings: Optional dictionary of ClickHouse settings (key/string values)
        :param query_formats: See QueryContext __init__ docstring
        :param column_formats: See QueryContext __init__ docstring.
        :param encoding: See QueryContext __init__ docstring
        :param context An alternative QueryContext parameter object that contains some or all of the method arguments
        :return: Numpy array representing the result set
        """
        return np_result(self.query(query,
                                    parameters,
                                    settings,
                                    query_formats,
                                    column_formats,
                                    encoding,
                                    False,
                                    context))

    # pylint: disable=duplicate-code
    def query_df(self,
                 query: str = None,
                 parameters: Optional[Union[Sequence, Dict[str, Any]]] = None,
                 settings: Optional[Dict[str, Any]] = None,
                 query_formats: Optional[Dict[str, str]] = None,
                 column_formats: Optional[Dict[str, str]] = None,
                 encoding: Optional[str] = None,
                 context: QueryContext = None):
        """
        Query method that results the results as a pandas dataframe
        :param query: Query statement/format string
        :param parameters: Optional dictionary used to format the query
        :param settings: Optional dictionary of ClickHouse settings (key/string values)
        :param query_formats: See QueryContext __init__ docstring
        :param column_formats: See QueryContext __init__ docstring
        :param encoding: See QueryContext __init__ docstring
        :param context An alternative QueryContext parameter object that contains some or all of the method arguments
        :return: Numpy array representing the result set
        """
        return to_pandas_df(self.query(query,
                                       parameters,
                                       settings,
                                       query_formats,
                                       column_formats,
                                       encoding,
                                       False,
                                       context))

    def query_arrow(self,
                    query: str,
                    parameters: Optional[Union[Sequence, Dict[str, Any]]] = None,
                    settings: Optional[Dict[str, Any]] = None,
                    use_strings: bool = True):
        """
        Query method using the ClickHouse Arrow format to return a PyArrow table
        :param query: Query statement/format string
        :param parameters: Optional dictionary used to format the query
        :param settings: Optional dictionary of ClickHouse settings (key/string values)
        :param use_strings:  Convert ClickHouse String type to Arrow string type (instead of binary)
        :return: PyArrow.Table
        """
        arrow_settings = {} if not settings else settings.copy()
        if 'output_format_arrow_string_as_string' not in arrow_settings:
            arrow_settings['output_format_arrow_string_as_string'] = '1' if use_strings else '0'
        return to_arrow(self.raw_query(query, parameters, arrow_settings, 'Arrow'))

    @abstractmethod
    def command(self,
                cmd: str,
                parameters: Optional[Union[Sequence, Dict[str, Any]]] = None,
                data: Union[str, bytes] = None,
                settings: Dict[str, Any] = None,
                use_database: bool = True) -> Union[str, int, Sequence[str]]:
        """
        Client method that returns a single value instead of a result set
        :param cmd: ClickHouse query/command as a python format string
        :param parameters: Optional dictionary of key/values pairs to be formatted
        :param data: Optional 'data' for the command (for INSERT INTO in particular)
        :param settings: Optional dictionary of ClickHouse settings (key/string values)
        :param use_database: Send the database parameter to ClickHouse so the command will be executed in that database
         context.  Otherwise, no database will be specified with the command.  This is useful for determining
         the default user database
        :return: Decoded response from ClickHouse as either a string, int, or sequence of strings
        """

    @abstractmethod
    def ping(self) -> bool:
        """
        Validate the connection, does not throw an Exception (see debug logs)
        :return: ClickHouse server is up and reachable
        """

    # pylint: disable=too-many-arguments
    def insert(self,
               table: str,
               data: Sequence[Sequence[Any]],
               column_names: Union[str, Iterable[str]] = '*',
               database: str = '',
               column_types: Sequence[ClickHouseType] = None,
               column_type_names: Sequence[str] = None,
               column_oriented: bool = False,
               settings: Optional[Dict[str, Any]] = None):
        """
        Method to insert multiple rows/data matrix of native Python objects
        :param table: Target table
        :param data: Sequence of sequences of Python data
        :param column_names: Ordered list of column names or '*' if column types should be retrieved from ClickHouse table definition
        :param database: Target database -- will use client default database if not specified
        :param column_types: ClickHouse column types.  If set then column data does not need to be retrieved from the server
        :param column_type_names: ClickHouse column type names.  If set then column data does not need to be retrieved from the server
        :param column_oriented: If true the data is already "pivoted" in column form
        :param settings: Optional dictionary of ClickHouse settings (key/string values)
        :return: No return, throws an exception if the insert fails
        """
        if len(data) == 0 or len(data[0]) == 0:
            logger.debug('Attempted insert with no data')
            return
        table, database, full_table = self.normalize_table(table, database)
        if isinstance(column_names, str):
            if column_names == '*':
                column_defs = [cd for cd in self.table_columns(table, database)
                               if cd.default_type not in ('ALIAS', 'MATERIALIZED')]
                column_names = [cd.name for cd in column_defs]
                column_types = [cd.ch_type for cd in column_defs]
            else:
                column_names = [column_names]
        elif len(column_names) == 0:
            raise ValueError('Column names must be specified for insert')
        if column_types is None:
            if column_type_names:
                column_types = [get_from_name(name) for name in column_type_names]
            else:
                column_map: Dict[str: ColumnDef] = {d.name: d for d in self.table_columns(table, database)}
                try:
                    column_types = [column_map[name].ch_type for name in column_names]
                except KeyError as ex:
                    raise ProgrammingError(f'Unrecognized column {ex} in table {table}') from None
        assert len(column_names) == len(column_types)
        self.data_insert(full_table, column_names, data, column_types, settings, column_oriented)

    def insert_df(self, table: str, data_frame, database: str = None, settings: Optional[Dict] = None):
        """
        Insert a pandas DataFrame into ClickHouse
        :param table: ClickHouse table
        :param data_frame: two-dimensional pandas dataframe
        :param database: Optional ClickHouse database
        :param settings: Optional dictionary of ClickHouse settings (key/string values)
        :return: No return, throws an exception if the insert fails
        """
        return self.insert(table, database=database, settings=settings, **from_pandas_df(data_frame))

    def insert_arrow(self,
                     table: str,
                     arrow_table,
                     database: str = None,
                     settings: Optional[Dict] = None):
        """
        Insert a PyArrow table DataFrame into ClickHouse using raw Arrow format
        :param table: ClickHouse table
        :param arrow_table: PyArrow Table object
        :param database: Optional ClickHouse database
        :param settings: Optional dictionary of ClickHouse settings (key/string values)
        :return: No return, throws an exception if the insert fails
        """
        _, _, full_table = self.normalize_table(table, database)
        column_names, insert_block = arrow_buffer(arrow_table)
        self.raw_insert(full_table, column_names, insert_block, settings, 'Arrow')

    def normalize_table(self, table: str, database: Optional[str]) -> Tuple[str, str, str]:
        """
        Convenience method to return the table, database, and full table name
        :param table: table name
        :param database: optional database
        :return: Tuple of bare table name, bare database name, and full database.table
        """
        split = table.split('.')
        if len(split) > 1:
            full_name = table
            database = split[0]
            table = split[1]
        else:
            name = table
            database = database or self.database
            full_name = f'{database}.{name}'
        return table, database, full_name

    def min_version(self, version_str: str) -> bool:
        """
        Determine whether the connected server is at least the submitted version
        :param version_str:  Version string consisting of up to 4 integers delimited by dots
        :return:  True version_str is greater than the server_version, False if less than
        """
        try:
            server_parts = [int(x) for x in self.server_version.split('.')]
            server_parts.extend([0] * (4 - len(server_parts)))
            version_parts = [int(x) for x in version_str.split('.')]
            version_parts.extend([0] * (4 - len(version_parts)))
        except ValueError:
            logger.warning('Server %s or requested version %s does not match format of numbers separated by dots',
                           self.server_version, version_str)
            return False
        for x, y in zip(server_parts, version_parts):
            if x > y:
                return True
            if x < y:
                return False
        return True

    def table_columns(self, table: str, database: str) -> Tuple[ColumnDef]:
        """
        Return complete column definitions for a ClickHouse table
        :param table: table name
        :param database: database name
        :return: list of ColumnDef named tuples
        """
        column_result = self.query(f'DESCRIBE TABLE {database}.{table}')
        if not column_result.result_set:
            raise InternalError(f'No table columns found for {database}.{table}')
        return tuple(ColumnDef(**row) for row in column_result.named_results())

    @abstractmethod
    def data_insert(self,
                    table: str,
                    column_names: Iterable[str],
                    data: Iterable[Iterable[Any]],
                    column_types: Iterable[ClickHouseType],
                    settings: Optional[Dict] = None,
                    column_oriented: bool = False):
        """
        Subclass implementation of the data insert
        :param table: ClickHouse table
        :param column_names: List of ClickHouse columns
        :param data: Data matrix
        :param column_types: Parallel list of ClickHouseTypes to insert
        :param settings:  Optional dictionary of ClickHouse settings (key/string values)
        :param column_oriented: Whether the data is already pivoted as a sequence of columns
        :return: No return, throws an exception if the insert fails
        """

    @abstractmethod
    def raw_insert(self, table: str,
                   column_names: Sequence[str],
                   insert_block: Union[str,  bytes],
                   settings: Optional[Dict] = None,
                   fmt: Optional[str] = None):
        """
        Insert data already formatted in a bytes object
        :param table: Table name (whether qualified with the database name or not)
        :param column_names: Sequence of column names
        :param insert_block: Binary or string data already in a recognized ClickHouse format
        :param settings:  Optional dictionary of ClickHouse settings (key/string values)
        :param fmt: Valid clickhouse format
        """

    def close(self):
        """
        Subclass implementation to close the connection to the server/deallocate the client
        """

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self.close()
