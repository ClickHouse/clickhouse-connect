from typing import NamedTuple, Any, Tuple, Dict, Collection

from clickhouse_connect.datatypes.base import ClickHouseType


class QueryResult(NamedTuple):
    result_set: Collection[Collection[Any]] = []
    column_names: Tuple[str] = []
    column_types: Tuple[ClickHouseType] = []
    query_id:str = None
    summary:Dict[str, Any] = {}


def transform_result(qr: QueryResult):
    for ix, t in enumerate(qr.column_types):
        if t.format:
            pass


