from typing import NamedTuple, Any, Iterable, Tuple, Dict

from clickhouse_connect.datatypes.registry import ClickHouseType


class QueryResult(NamedTuple):
    result_set: Iterable[Iterable[Any]] = []
    column_names: Tuple[str] = []
    column_types: Tuple[ClickHouseType] = []
    query_id:str = None
    summary:Dict[str, Any] = {}

