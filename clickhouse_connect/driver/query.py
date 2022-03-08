from typing import NamedTuple, Any, List, Dict

from clickhouse_connect.datatypes.registry import ClickHouseType


class QueryResult(NamedTuple):
    result_set: List[List[Any]] = []
    column_names: List[str] = []
    column_types: List[ClickHouseType] = []
    query_id:str = None
    summary:Dict[str, Any] = {}


class DataInsert(NamedTuple):
    table: str
    data = List[List[Any]]
    column_names: List[str] = None
    column_types: List[str] = None
    database: str = None






