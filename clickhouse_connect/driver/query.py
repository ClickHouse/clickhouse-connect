from collections.abc import Sequence
from typing import NamedTuple, Any, Tuple, Dict

from clickhouse_connect.datatypes.base import ClickHouseType


class QueryResult(NamedTuple):
    result_set: Sequence[Sequence[Any]] = []
    column_names: Tuple[str] = []
    column_types: Tuple[ClickHouseType] = []
    query_id: str = None
    summary: Dict[str, Any] = {}
