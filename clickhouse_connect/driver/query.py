import ipaddress
import uuid

from enum import Enum
from typing import NamedTuple, Any, Tuple, Dict, Sequence
from datetime import date, datetime
from pytz import UTC

from clickhouse_connect.datatypes.base import ClickHouseType
from clickhouse_connect.driver.options import HAS_NUMPY, HAS_PANDAS, check_pandas, check_numpy

if HAS_PANDAS:
    import pandas as pa

if HAS_NUMPY:
    import numpy as np


class QueryResult():
    def __init__(self, result_set: Sequence[Sequence[Any]], column_names: Tuple[str, ...],
                 column_types: Tuple[ClickHouseType, ...], query_id: str = None, summary: Dict[str, Any] = None):
        self.result_set = result_set
        self.column_names = column_names
        self.column_types = column_types
        self.query_id = query_id
        self.summary = summary

    def named_results(self):
        for ix, row in enumerate(self.result_set):
            x = dict(zip(self.column_names[ix], row))
            x['@type'] = self.column_types[ix]
            yield x


class DataResult(NamedTuple):
    result: Sequence[Sequence[Any]]
    column_names: Tuple[str]
    column_types: Tuple[ClickHouseType]


local_tz = datetime.now().astimezone().tzinfo
BS = '\\'
must_escape = (BS, '\'')


# pylint: disable=too-many-return-statements
def escape_query_value(value, server_tz=UTC):
    if value is None:
        return 'NULL'

    if isinstance(value, str):
        return f"'{''.join(f'{BS}{c}' if c in must_escape else c for c in value)}'"

    if isinstance(value, date):
        return f"'{value.isoformat()}'"

    if isinstance(value, datetime):
        if value.tzinfo is None and server_tz != local_tz:
            value = value.replace(tzinfo=server_tz)
        return f"'{value.strftime('%Y-%m-%d %H:%M:%S%')}'"

    if isinstance(value, list):
        return f"[{', '.join(escape_query_value(x, server_tz) for x in value)}]"

    if isinstance(value, tuple):
        return f"({', '.join(escape_query_value(x, server_tz) for x in value)})"

    if isinstance(value, dict):
        pairs = [escape_query_value(k, server_tz) + ':' + escape_query_value(v, server_tz) for k, v in value.items()]
        return f"{{{', '.join(pairs)}}}"

    if isinstance(value, Enum):
        return escape_query_value(value.value, server_tz)

    if isinstance(value, (uuid.UUID, ipaddress.IPv4Address, ipaddress.IPv6Address)):
        return f"'{value}'"

    return str(value)


def np_result(result: QueryResult) -> 'np.array':
    check_numpy()
    np_types = [(name, ch_type.np_type) for name, ch_type in zip(result.column_names, result.column_types)]
    return np.array(result.result_set, dtype=np_types)


def to_pandas_df(result: QueryResult) -> 'pa.DataFrame':
    check_pandas()
    return pa.DataFrame(np_result(result))


def from_pandas_df(df: 'pa.DataFrame'):
    check_pandas()
    return {'column_names': df.columns, 'data': df.to_numpy()}
