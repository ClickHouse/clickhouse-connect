from typing import Union, Iterable, Sequence, Optional, Any, Dict, NamedTuple, Generator

from clickhouse_connect.datatypes.base import ClickHouseType
from clickhouse_connect.driver.common import SliceView

DEFAULT_BLOCK_SIZE = 16834


class InsertBlock(NamedTuple):
    column_count: int
    row_count: int
    column_names: Iterable[str]
    column_types: Iterable[ClickHouseType]
    column_data: Iterable[Sequence[Any]]


class InsertContext:
    """
    Reusable Argument/parameter object for inserts.
    """

    _block_columns = []
    _block_rows = []

    def __init__(self,
                 data: Sequence[Sequence[Any]],
                 column_names: Sequence[str],
                 column_types: Sequence[ClickHouseType],
                 table: str = None,
                 column_oriented: bool = False,
                 allow_nulls: bool = True,
                 settings: Optional[Dict[str, Any]] = None,
                 compression: Optional[str] = None,
                 block_size:int = DEFAULT_BLOCK_SIZE):
        self.table = table
        self.column_names = column_names
        self.column_types = column_types
        self.column_oriented = column_oriented
        self.settings = settings
        self.allow_nulls = allow_nulls
        self.compression = compression
        self.current_block = 0
        self.current_row = 0
        self.block_size = block_size
        if data:
            if self.column_oriented:
                self._next_block_data = self._column_block_data
                self._block_columns = [SliceView(column) for column in data]
                self.column_count = len(data)
                self.row_count = len(data[0])
            else:
                self._next_block_data = self._row_block_data
                self._block_rows = data
                self.row_count = len(data)
                self.column_count = len(data[0])
        else:
            self.row_count = 0
            self.column_count = 0

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

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

