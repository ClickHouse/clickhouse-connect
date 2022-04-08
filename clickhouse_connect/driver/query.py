from collections.abc import Sequence
from typing import NamedTuple, Any, Tuple, Dict

from clickhouse_connect.datatypes.base import ClickHouseType
from clickhouse_connect.driver.options import HAS_NUMPY, HAS_PANDAS, check_pandas, check_numpy

if HAS_PANDAS:
    import pandas as pa

if HAS_NUMPY:
    import numpy as np


class QueryResult(NamedTuple):
    result_set: Sequence[Sequence[Any]] = []
    column_names: Tuple[str] = []
    column_types: Tuple[ClickHouseType] = []
    query_id: str = None
    summary: Dict[str, Any] = {}


def np_result(result: QueryResult) -> 'np.array':
    check_numpy()
    np_types = [(name, ch_type.np_type) for name, ch_type in zip(result.column_names, result.column_types)]
    return np.array(result.result_set, dtype=np_types)


def to_pandas_df(result: QueryResult) -> 'pa.DataFrame':
    check_pandas()
    return pa.DataFrame(np_result(result))


def from_pandas_df(df: 'pa.DataFrame'):
    check_pandas()
    return {
        'column_names': df.columns,
        'data': df.to_numpy()
    }
