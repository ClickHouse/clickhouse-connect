import logging
import pytz

from abc import ABC, abstractmethod
from typing import Iterable, Optional, Any, Union, Sequence, Dict
from pytz.exceptions import UnknownTimeZoneError

from clickhouse_connect.datatypes.registry import get_from_name
from clickhouse_connect.datatypes.base import ClickHouseType
from clickhouse_connect.driver.exceptions import ProgrammingError
from clickhouse_connect.driver.insert import InsertContext
from clickhouse_connect.driver.models import ColumnDef, SettingDef
from clickhouse_connect.driver.query import QueryResult, np_result, to_pandas_df, to_arrow, \
    QueryContext, arrow_buffer

logger = logging.getLogger(__name__)


class Client(ABC):
    """
    Base ClickHouse Connect client
    """
    column_inserts = False
    compression = None
    generate_session_id = True
    valid_transport_settings = set()

    def __init__(self, database: str, query_limit: int, uri: str, compression: Optional[str]):
        """
        Shared initialization of ClickHouse Connect client
        :param database: database name
        :param query_limit: default LIMIT for queries
        :param uri: uri for error messages
        """
        self.query_limit = query_limit
        self.server_tz = pytz.UTC
        self.server_version, server_tz, self.database = \
            tuple(self.command('SELECT version(), timezone(), database()', use_database=False))
        try:
            self.server_tz = pytz.timezone(server_tz)
        except UnknownTimeZoneError:
            logger.warning('Warning, server is using an unrecognized timezone %s, will use UTC default', server_tz)
        server_settings = self.query('SELECT name, value, changed, description, type, readonly FROM system.settings')
        self.server_settings = {row['name']: SettingDef(**row) for row in server_settings.named_results()}
        if compression and self.server_settings.get('enable_http_compression', False):
            self.compression = compression
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
        if context.is_select and not context.has_limit and self.query_limit:
            return f'{context.final_query}\n LIMIT {self.query_limit}'
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
              column_oriented: bool = False,
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
        :param column_oriented: True if the result should be sequence of column values, False for rows
        :param context An alternative QueryContext parameter object that contains some or all of the method arguments
        :return: QueryResult -- data and metadata from response
        """
        if context:
            query_context = context.updated_copy(query=query,
                                                 parameters=parameters,
                                                 settings=settings,
                                                 query_formats=query_formats,
                                                 column_formats=column_formats,
                                                 encoding=encoding,
                                                 server_tz=self.server_tz,
                                                 use_none=use_none,
                                                 column_oriented=column_oriented)
        else:
            query_context = QueryContext(query=query,
                                         parameters=parameters,
                                         settings=settings,
                                         query_formats=query_formats,
                                         column_formats=column_formats,
                                         encoding=encoding,
                                         server_tz=self.server_tz,
                                         use_none=use_none,
                                         column_oriented=column_oriented)
        if query_context.is_command:
            response = self.command(query, parameters=query_context.parameters, settings=query_context.settings)
            return QueryResult([response] if isinstance(response, list) else [[response]], (), ())
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
                 column_oriented: bool = False,
                 context: QueryContext = None):
        """
        Query method that returns the results as a numpy array
        :param query: Query statement/format string
        :param parameters: Optional dictionary used to format the query
        :param settings: Optional dictionary of ClickHouse settings (key/string values)
        :param query_formats: See QueryContext __init__ docstring
        :param column_formats: See QueryContext __init__ docstring.
        :param encoding: See QueryContext __init__ docstring
        :param column_oriented: True if the result should be sequence of column values, False for rows
        :param context An alternative QueryContext parameter object that contains some or all of the method arguments
        :return: Numpy array representing the result set
        """
        if context:
            return self.query(context=context)
        return np_result(self.query(query=query,
                                    parameters=parameters,
                                    settings=settings,
                                    query_formats=query_formats,
                                    column_formats=column_formats,
                                    encoding=encoding,
                                    use_none=False,
                                    column_oriented=column_oriented))

    # pylint: disable=duplicate-code
    def query_df(self,
                 query: str = None,
                 parameters: Optional[Union[Sequence, Dict[str, Any]]] = None,
                 settings: Optional[Dict[str, Any]] = None,
                 query_formats: Optional[Dict[str, str]] = None,
                 column_formats: Optional[Dict[str, str]] = None,
                 encoding: Optional[str] = None,
                 allow_nulls: bool = True,
                 context: QueryContext = None):
        """
        Query method that results the results as a pandas dataframe
        :param query: Query statement/format string
        :param parameters: Optional dictionary used to format the query
        :param settings: Optional dictionary of ClickHouse settings (key/string values)
        :param query_formats: See QueryContext __init__ docstring
        :param column_formats: See QueryContext __init__ docstring
        :param encoding: See QueryContext __init__ docstring
        :param allow_nulls: Interpret ClickHouse nulls as NaT/NA pandas values, otherwise the DataFrame values
           will be the default "non-null' values such as empty string or 0
        :param context An alternative QueryContext parameter object that contains some or all of the method arguments
        :return: Numpy array representing the result set
        """
        return to_pandas_df(self.query(query,
                                       parameters,
                                       settings,
                                       query_formats,
                                       column_formats,
                                       encoding,
                                       use_none=allow_nulls,
                                       column_oriented=True,
                                       context=context))

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
               table: Optional[str] = None,
               data: Sequence[Sequence[Any]] = None,
               column_names: Union[str, Iterable[str]] = '*',
               database: str = '',
               column_types: Sequence[ClickHouseType] = None,
               column_type_names: Sequence[str] = None,
               column_oriented: bool = False,
               settings: Optional[Dict[str, Any]] = None,
               context: InsertContext = None) -> None:
        """
        Method to insert multiple rows/data matrix of native Python objects.  If context is specified arguments
        other than data are ignored
        :param table: Target table
        :param data: Sequence of sequences of Python data
        :param column_names: Ordered list of column names or '*' if column types should be retrieved from the
            ClickHouse table definition
        :param database: Target database -- will use client default database if not specified
        :param column_types: ClickHouse column types.  If set then column data does not need to be retrieved from
            the server
        :param column_type_names: ClickHouse column type names.  If set then column data does not need to be
            retrieved from the server
        :param column_oriented: If true the data is already "pivoted" in column form
        :param settings: Optional dictionary of ClickHouse settings (key/string values)
        :param context: Optional reusable insert context to allow repeated inserts into the same table with
            different data batches
        :return: No return, throws an exception if the insert fails
        """
        if context:
            if context:
                if context.data is None and data is None:
                    raise ProgrammingError('No data specified for insert') from None
        else:
            context = self.create_insert_context(table,
                                                 database,
                                                 column_names,
                                                 column_types,
                                                 column_type_names,
                                                 column_oriented,
                                                 settings)
        context.data = data
        self.data_insert(context)

    def insert_df(self, table: str = None,
                  df=None,
                  database: str = None,
                  settings: Optional[Dict] = None,
                  column_names: Optional[Sequence[str]] = None,
                  context: InsertContext = None) -> None:
        """
        Insert a pandas DataFrame into ClickHouse.  If context is specified arguments other than df are ignored
        :param table: ClickHouse table
        :param df: two-dimensional pandas dataframe
        :param database: Optional ClickHouse database
        :param settings: Optional dictionary of ClickHouse settings (key/string values)
        :param column_names: An optional list of ClickHouse column names.  If not set, the DataFrame column names
           will be used
        :param context: Optional reusable insert context to allow repeated inserts into the same table with
            different data batches
        :return: No return, throws an exception if the insert fails
        """
        if context:
            if context.data is None and df is None:
                raise ProgrammingError('No data specified for insert') from None
        else:
            context = self.create_pandas_insert_context(table, df, database, settings, column_names)
        context.df = df
        self.data_insert(context)

    def insert_arrow(self, table: str, arrow_table, database: str = None, settings: Optional[Dict] = None):
        """
        Insert a PyArrow table DataFrame into ClickHouse using raw Arrow format
        :param table: ClickHouse table
        :param arrow_table: PyArrow Table object
        :param database: Optional ClickHouse database
        :param settings: Optional dictionary of ClickHouse settings (key/string values)
        :return: No return, throws an exception if the insert fails
        """
        full_table = table if '.' in table else f'{database or self.database}.{table}'
        column_names, insert_block = arrow_buffer(arrow_table)
        self.raw_insert(full_table, column_names, insert_block, settings, 'Arrow')

    def create_insert_context(self,
                              table: str,
                              database: str = '',
                              column_names: Union[str, Sequence[str]] = '*',
                              column_types: Sequence[ClickHouseType] = None,
                              column_type_names: Sequence[str] = None,
                              column_oriented: bool = False,
                              settings: Optional[Dict[str, Any]] = None,
                              data: Optional[Sequence[Sequence[Any]]] = None) -> InsertContext:
        """
        Builds a reusable insert context to hold state for a duration of an insert
        :param table: Target table
        :param database: Target database.  If not set, uses the client default database
        :param column_names: Ordered list of column names or '*' if column types should be retrieved from ClickHouse table definition
        :param database: Target database -- will use client default database if not specified
        :param column_types: ClickHouse column types.  If set then column data does not need to be retrieved from the server
        :param column_type_names: ClickHouse column type names.  If set then column data does not need to be retrieved from the server
        :param column_oriented: If true the data is already "pivoted" in column form
        :param settings: Optional dictionary of ClickHouse settings (key/string values)
        :param data: Initial dataset for insert
        :return Reusable insert context
        """
        full_table = table if '.' in table else f'{database or self.database}.{table}'
        column_defs = []
        if column_types is None:
            describe_result = self.query(f'DESCRIBE TABLE {full_table}')
            column_defs = [ColumnDef(**row) for row in describe_result.named_results()
                           if row['default_type'] not in ('ALIAS', 'MATERIALIZED')]
        if isinstance(column_names, str):
            if column_names == '*':
                column_names = [cd.name for cd in column_defs]
                column_types = [cd.ch_type for cd in column_defs]
            elif column_names:
                column_names = [column_names]
        if len(column_names) == 0:
            raise ValueError('Column names must be specified for insert')
        if not column_types:
            if column_type_names:
                column_types = [get_from_name(name) for name in column_type_names]
            else:
                column_map = {d.name: d for d in column_defs}
                try:
                    column_types = [column_map[name].ch_type for name in column_names]
                except KeyError as ex:
                    raise ProgrammingError(f'Unrecognized column {ex} in table {table}') from None
        if len(column_names) != len(column_types):
            raise ProgrammingError('Column names do not match column types') from None
        return InsertContext(full_table,
                             column_names,
                             column_types,
                             column_oriented=column_oriented,
                             settings=settings,
                             data=data)

    def create_pandas_insert_context(self,
                                     table: str,
                                     df=None,
                                     database: str = '',
                                     settings: Optional[Dict] = None,
                                     column_names: Optional[Sequence[str]] = None) -> InsertContext:
        """
        Convenience method to create a reusable insert context from a Pandas DataFrame.  It does not
        set the context data
        :param table: ClickHouse table
        :param df: two-dimensional pandas dataframe
        :param database: Optional ClickHouse database
        :param settings: Optional dictionary of ClickHouse settings (key/string values)
        :param column_names: An optional list of ClickHouse column names.  If not set, the DataFrame column names
           will be used
        :return: Reusable pandas insert context
        """
        if df is None and not column_names:
            raise ProgrammingError('Either a sample df or column names must be specified')
        if column_names:
            if df is not None and len(column_names) != len(df.columns):
                raise ProgrammingError('DataFrame column count does not match insert_columns') from None
        else:
            column_names = df.columns
        return self.create_insert_context(table, database, column_names, column_oriented=True, settings=settings)

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

    @abstractmethod
    def data_insert(self, context: InsertContext):
        """
        Subclass implementation of the data insert
        :context: InsertContext parameter object
        :return: No return, throws an exception if the insert fails
        """

    @abstractmethod
    def raw_insert(self, table: str,
                   column_names: Sequence[str],
                   insert_block: Union[str, bytes],
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
