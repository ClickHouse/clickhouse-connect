import datetime
from typing import Iterable, Sequence, Optional, Any, Dict, NamedTuple, Generator, Union

from clickhouse_connect.datatypes.base import ClickHouseType
from clickhouse_connect.datatypes.registry import get_from_name
from clickhouse_connect.driver.common import SliceView
from clickhouse_connect.driver.context import BaseQueryContext
from clickhouse_connect.driver.options import np, pd
from clickhouse_connect.driver.exceptions import ProgrammingError

DEFAULT_BLOCK_SIZE = 16834
dt_nano_type = get_from_name('DateTime64(9)')


class InsertBlock(NamedTuple):
    column_count: int
    row_count: int
    column_names: Iterable[str]
    column_types: Iterable[ClickHouseType]
    column_data: Iterable[Sequence[Any]]


# pylint: disable=too-many-instance-attributes
class InsertContext(BaseQueryContext):
    """
    Reusable Argument/parameter object for inserts.
    """
    # pylint: disable=too-many-arguments
    def __init__(self,
                 table: str,
                 column_names: Sequence[str],
                 column_types: Sequence[ClickHouseType],
                 data: Any = None,
                 column_oriented: bool = False,
                 settings: Optional[Dict[str, Any]] = None,
                 compression: Optional[str] = None,
                 query_formats: Optional[Dict[str, str]] = None,
                 column_formats: Optional[Dict[str, Union[str, Dict[str, str]]]] = None,
                 block_size: int = DEFAULT_BLOCK_SIZE):
        super().__init__(settings, query_formats, column_formats)
        self.table = table
        self.column_names = column_names
        self.column_types = column_types
        self.column_oriented = column_oriented
        self.compression = compression
        self.block_size = block_size
        self.data = data
        self.insert_exception = None

    @property
    def empty(self) -> bool:
        return self._data is None

    @property
    def data(self):
        return self._raw_data

    @data.setter
    def data(self, data: Any):
        self._raw_data = data
        self.current_block = 0
        self.current_row = 0
        self.row_count = 0
        self.column_count = 0
        self._data = None
        if data is None or len(data) == 0:
            return
        if pd and isinstance(data, pd.DataFrame):
            data = self._convert_pandas(data)
            self.column_oriented = True
        if np and isinstance(data, np.ndarray):
            data = self._convert_numpy(data)
            self.column_oriented = True
        if self.column_oriented:
            self._next_block_data = self._column_block_data
            self._block_columns = [SliceView(column) for column in data]
            self._block_rows = None
            self.column_count = len(data)
            self.row_count = len(data[0])
        else:
            self._next_block_data = self._row_block_data
            self._block_rows = data
            self._block_columns = None
            self.row_count = len(data)
            self.column_count = len(data[0])
        if self.row_count and self.column_count:
            if self.column_count != len(self.column_names):
                raise ProgrammingError('Insert data column count does not match column names')
            self._data = data

    def next_block(self) -> Generator[InsertBlock, None, None]:
        while True:
            block_end = min(self.current_row + self.block_size, self.row_count)
            row_count = block_end - self.current_row
            if row_count <= 0:
                return
            self.current_block += 1
            data = self._next_block_data(self.current_row, block_end)
            yield InsertBlock(self.column_count, row_count, self.column_names, self.column_types, data)
            self.current_row = block_end

    def _column_block_data(self, block_start, block_end):
        return [col[block_start: block_end] for col in self._block_columns]

    def _row_block_data(self, block_start, block_end):
        column_slice = SliceView(self._block_rows[block_start: block_end])
        return tuple(zip(*column_slice))

    def _convert_pandas(self, df):
        data = []
        for df_col_name, col_name, ch_type in zip(df.columns, self.column_names, self.column_types):
            df_col = df[df_col_name]
            d_type = str(df_col.dtype)
            if ch_type.python_type == int:
                if 'float' in d_type:
                    df_col = df_col.round().astype(ch_type.base_type, copy=False)
                else:
                    df_col = df_col.astype(ch_type.base_type, copy=False)
            elif ch_type == dt_nano_type and pd.core.dtypes.common.is_datetime64_ns_dtype(df_col):
                data.append([None if pd.isnull(x) else x.value for x in df_col])
                self.column_formats[col_name] = 'int'
                continue
            elif ch_type.python_type in (datetime.datetime, datetime.date) and 'date' in d_type:
                data.append([None if pd.isnull(x) else pd.Timestamp.to_pydatetime(x) for x in df_col])
                continue
            if ch_type.nullable and ch_type.python_type != float:
                df_col.replace({np.nan: None}, inplace=True)
            data.append(df_col.tolist())
        return data

    def _convert_numpy(self, np_array):
        data = []
        if np_array.dtype.names is not None:
            # This is a structured array, so get column views on the underlying structure
            for field in np_array.dtype.names:
                data.append(np_array[field])
        else:
            for ix, (col_name, ch_type) in enumerate(zip(self.column_names, self.column_types)):
                np_data = np_array[ix, :] if self.column_oriented else np_array[:, ix]
                if ch_type == dt_nano_type:
                    self.column_formats[col_name] = 'int'
                data.append(np_data)
        return data
