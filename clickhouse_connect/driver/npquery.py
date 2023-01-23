import logging
from typing import Generator, Sequence, Tuple, Dict, Any, Iterator

import numpy as np

from clickhouse_connect.datatypes.base import ClickHouseType
from clickhouse_connect.driver import ProgrammingError
from clickhouse_connect.driver.types import Closable, Matrix

logger = logging.getLogger(__name__)


class NumpyResult:
    def __init__(self,
                 block_gen: Generator[Sequence[np.array], None, None] = None,
                 column_names: Tuple[str, ...] = (),
                 column_types: Tuple[ClickHouseType, ...] = (),
                 max_str_len: int = 0,
                 source: Closable = None):
        self.column_names = column_names
        self.column_types = column_types
        self.np_types = [col_type.np_type(max_str_len) for col_type in column_types]
        self.source = source
        self.query_id = ''
        self.summary = {}
        self._result_columns = None
        self._block_gen = block_gen or iter(())
        self._in_context = False
        self._matrix = False
        self._matrix_type = None
        first_type = np.dtype(self.np_types[0])
        if first_type != np.object_ and all(np.dtype(np_type) == first_type for np_type in self.np_types):
            self._matrix = True
            self._matrix_type = first_type


    def _block_to_numpy(self, block: Sequence[np.array]) -> np.array:


    def stream_blocks(self) -> Iterator[np.array]:
        if not self._in_context:
            logger.warning("Streaming results should be used in a 'with' context to ensure the stream is closed")
        if not self._block_gen:
            raise ProgrammingError('Stream closed')
        temp = self._block_gen
        self._block_gen = None
        return temp

    def close(self):
        if self.source:
            self.source.close()
            self.source = None

    def __enter__(self):
        self._in_context = True

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        self._in_context = False

