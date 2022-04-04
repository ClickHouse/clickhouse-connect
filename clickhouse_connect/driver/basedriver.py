from abc import ABCMeta, abstractmethod
from typing import Iterable, Tuple, Optional, Any, Union, NamedTuple

from clickhouse_connect.datatypes.registry import get_from_name
from clickhouse_connect.datatypes.base import ClickHouseType
from clickhouse_connect.driver.exceptions import ServerError, OperationError
from clickhouse_connect.driver.query import QueryResult, np_result, to_pandas_df, from_pandas_df


class ColumnDef(NamedTuple):
    name: str
    type: ClickHouseType
    default_kind: str
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
    def __init__(self, database: str, query_limit: int):
        if database and not database == '__default__':
            self._database = database
        self.limit = query_limit

    @property
    def database(self):
        if not hasattr(self, '_database'):
            self._database = self.command('SELECT database()')
        return self._database

    def query(self, query: str, parameters=None, use_none: bool = True) -> QueryResult:
        query = query.replace('\n', ' ')
        if self.limit and ' LIMIT ' not in query.upper() and 'SELECT ' in query.upper():
            query += f' LIMIT {self.limit}'
        return self.exec_query(query, use_none)

    def query_np(self, query: str):
        return np_result(self.query(query, use_none=False))

    def query_df(self, query: str):
        return to_pandas_df(self.query(query, use_none=False))

    def insert_df(self, table: str, df):
        insert = from_pandas_df(df)
        return self.insert(table, **insert)

    @abstractmethod
    def exec_query(self, query: str, use_none: bool = True) -> QueryResult:
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
        table, database, full_table = self.normalize_table(table, database)
        if isinstance(column_names, str):
            if column_names == '*':
                column_defs = [cd for cd in self.table_columns(table, database)
                               if cd.default_kind not in ('ALIAS', 'MATERIALIZED')]
                column_names = [cd.name for cd in column_defs]
                column_types = [cd.type for cd in column_defs]
            else:
                column_names = [column_names]
        elif len(column_names) == 0:
            raise ValueError("Column names must be specified for insert")
        if column_types is None:
            if column_type_names:
                column_types = [get_from_name(name) for name in column_type_names]
            else:
                column_map: dict[str: ColumnDef] = {d.name: d for d in self.table_columns(table, database)}
                try:
                    column_types = [column_map[name].type for name in column_names]
                except KeyError as ke:
                    raise OperationError(f"Unrecognized column {ke} in table {table}")
        assert len(column_names) == len(column_types)
        self.data_insert(full_table, column_names, data, column_types)

    def normalize_table(self, table: str, database: str) -> Tuple[str, str, str]:
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

    def table_columns(self, table: str, database: str) -> Tuple[ColumnDef]:
        column_result = self.query(
            "SELECT name, type, default_kind, default_kind, default_expression, compression_codec, comment "
            f"FROM system.columns WHERE database = '{database}' and table = '{table}'  ORDER BY position")
        if not column_result.result_set:
            raise ServerError(f'No table columns found for {database}.{table}')
        return tuple([ColumnDef(row[0], get_from_name(row[1]), row[2], row[3], row[4], row[5])
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

    def __enter__(self):
        return self

    def __exit__(self):
        self.close()
