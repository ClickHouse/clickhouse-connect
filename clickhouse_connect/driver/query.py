from collections.abc import Sequence
from typing import NamedTuple, Any, Tuple, Dict

from clickhouse_connect.datatypes.base import ClickHouseType

try:
    import numpy as np
except ImportError:
    pass

try:
    import pandas as pa
except ImportError:
    pass


class QueryResult(NamedTuple):
    result_set: Sequence[Sequence[Any]] = []
    column_names: Tuple[str] = []
    column_types: Tuple[ClickHouseType] = []
    query_id: str = None
    summary: Dict[str, Any] = {}


def np_result(result: QueryResult) -> np.array:
    np_types = [(name, ch_type.np_type) for name, ch_type in zip(result.column_names, result.column_types)]
    return np.array(result.result_set, dtype=np_types)


def pandas_df(result: QueryResult) -> pa.DataFrame:
    return pa.DataFrame(np_result(result))
