from abc import ABCMeta, abstractmethod
from typing import Iterable, Tuple, Optional, Any, Union, NamedTuple

from clickhouse_connect.datatypes.registry import ClickHouseType, get_from_name
from clickhouse_connect.driver.exceptions import ServerError
from clickhouse_connect.driver.query import QueryResult


class ColumnDef(NamedTuple):
    name: str
    type: ClickHouseType
    default_expression: str
    compression_codec: str
    comment: str


class TableDef(NamedTuple):
    database: str
    name: str
    column_defs: tuple[ColumnDef]
    engine: str
    order_by: str
    sort_by: str
    comment: str

    @property
    def column_names(self):
        return (t.name for t in self.column_defs)

    def column_types(self):
        return (t.type for t in self.column_defs)


class BaseDriver(metaclass=ABCMeta):

    def __init__(self, database: str, **kwargs):
        if database and not database == '__default__':
            self._database = database

    def __enter__(self):
        return self

    def __exit__(self):
        self.close()

    @property
    def database(self):
        if not hasattr(self, '_database'):
            self._database = self.command('SELECT database()')
        return self._database

    @abstractmethod
    def query(self, query: str, ) -> QueryResult:
        pass

    @abstractmethod
    def command(self, cmd: str) -> str:
        pass

    @abstractmethod
    def ping(self):
        pass

    def insert(self, table: str, column_names: Union[str or Iterable[str]], data: Iterable[Iterable[Any]],
               database: str = '', column_types: Optional[Iterable[ClickHouseType]] = None,
               column_type_names: Optional[Iterable[str]] = None):
        table_name, database, full_name = self.normalize_table(table, database)
        if column_names == '*':
            column_defs = self.table_columns(table_name, database)
            column_names = [cd.name for cd in column_defs]
            column_types = [cd.type for cd in column_defs]
        elif not column_names:
            raise ValueError("Column names must be specified for insert")
        elif not column_types:
            if column_type_names:
                column_types = [get_from_name(name) for name in column_type_names]
            else:
                column_defs = self.table_columns(table_name, database)
                column_types = [cd.type for cd in column_defs]
        self.data_insert(full_name, column_names, data, column_types)

    def normalize_table(self, table: str, database: str) -> Tuple[str, str, str]:
        split = table.split('.')
        if len(split) > 1:
            database = split[0]
            name = split[1]
        else:
            name = table
            database = database or self.database
            if database:
                full_name = f'{database}.{name}'
        return name, database, full_name

    def table_columns(self, table_name: str, database: str) -> Tuple[ColumnDef]:
        column_result = self.query(
            "SELECT name, type, default_expression, compression_codec, comment FROM system.columns"
            f" WHERE database = '{database}' and table = '{table_name}' and default_kind = '' ORDER BY position")
        if not column_result.result_set:
            raise ServerError(f'No table columns found for {database}.{table_name}')
        return tuple([ColumnDef(row[0], get_from_name(row[1]), row[2], row[3], row[4])
                      for row in column_result.result_set])

    @abstractmethod
    def data_insert(self, table: str, column_names: Iterable[str], data: Iterable[Iterable[Any]],
                    column_types: Iterable[ClickHouseType]):
        pass

    @abstractmethod
    def raw_request(self, data=None, **kwargs) -> Any:
        pass

    def close(self):
        pass
