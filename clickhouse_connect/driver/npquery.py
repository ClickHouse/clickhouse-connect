import logging
from typing import Generator, Sequence, Tuple, Dict, Any, Iterator

from clickhouse_connect.datatypes.base import ClickHouseType
from clickhouse_connect.driver import ProgrammingError
from clickhouse_connect.driver.types import Closable
from clickhouse_connect.driver.options import np

logger = logging.getLogger(__name__)


class NumpyResult:
    def __init__(self,
                 block_gen: Generator[Sequence[np.array], None, None] = None,
                 column_names: Tuple[str, ...] = (),
                 column_types: Tuple[ClickHouseType, ...] = (),
                 use_none: bool = False,
                 max_str_len: int = 0,
                 source: Closable = None):
        self.column_names = column_names
        self.column_types = column_types
        self.use_none = use_none
        self.source = source
        self.np_types = [col_type.np_type(max_str_len) for col_type in column_types]
        self.d_types = np.dtype(list(zip(column_names, self.np_types)))
        self.query_id = ''
        self.summary = {}
        self._result_columns = None
        if block_gen:
            def numpy_blocks():
                for block in block_gen:
                    yield self._block_to_numpy(block)
            self._block_gen = numpy_blocks()
        else:
            self._block_gen = iter(())
        self._in_context = False
        self._matrix = False
        self._dtype = None
        self._has_objects = False
        first_type = np.dtype(self.np_types[0])
        if first_type != np.object_ and all(np.dtype(np_type) == first_type for np_type in self.np_types):
            self._matrix = True
            self._dtype = first_type

    def _block_to_numpy(self, block: Sequence[np.array]) -> np.array:
        if self._matrix:
            return np.array(block, self._dtype).transpose()
        columns = []
        if self._has_objects:
            return np.rec.fromarrays(block, self.d_types)
        for column, col_type, np_type in zip(block, self.column_types, self.np_types):
            if np_type == 'O':
                columns.append(column)
                self._has_objects = True
            elif self.use_none and col_type.nullable:
                new_col = []
                item_array = np.empty(1, dtype=np_type)
                for x in column:
                    if x is None:
                        new_col.append(None)
                        self._has_objects = True
                    else:
                        item_array[0] = x
                        new_col.append(item_array[0])
                columns.append(new_col)
            elif 'date' in np_type:
                columns.append(np.array(column, dtype=np_type))
            else:
                columns.append(column)
        if self._has_objects:
            self.np_types = [np.object_] * len(self.column_names)
            self.d_types = np.dtype(list(zip(self.column_names, self.np_types)))
            self._dtype = object
        return np.rec.fromarrays(columns, self.d_types)

    def stream_blocks(self) -> Iterator[np.array]:
        if not self._in_context:
            logger.warning("Streaming results should be used in a 'with' context to ensure the stream is closed")
        if not self._block_gen:
            raise ProgrammingError('Stream closed')
        temp = self._block_gen
        self._block_gen = None
        return temp

    @property
    def result_set(self):
        if not self._block_gen:
            raise ProgrammingError('Stream closed')
        pieces = [arr for arr in self._block_gen]
        self._block_gen = None
        return np.concatenate(pieces, dtype=self._dtype)

    def close(self):
        if self.source:
            self.source.close()
            self.source = None

    def __enter__(self):
        self._in_context = True

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        self._in_context = False
