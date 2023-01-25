import logging
from typing import Generator, Sequence, Tuple, Dict, Any, Iterator

from clickhouse_connect.datatypes.base import ClickHouseType
from clickhouse_connect.driver.common import empty_gen
from clickhouse_connect.driver.exceptions import ProgrammingError
from clickhouse_connect.driver.types import Closable
from clickhouse_connect.driver.options import np, pd

logger = logging.getLogger(__name__)


class NumpyResult:
    def __init__(self,
                 block_gen: Generator[Sequence[np.array], None, None] = None,
                 column_names: Tuple[str, ...] = (),
                 column_types: Tuple[ClickHouseType, ...] = (),
                 d_types: Sequence[np.dtype] = (),
                 source: Closable = None):
        self.column_names = column_names
        self.column_types = column_types
        self.np_types = d_types
        self.source = source
        self.query_id = ''
        self.summary = {}
        self._block_gen = block_gen or empty_gen()
        self._numpy_result = None
        self._in_context = False

        if block_gen:
            first_type = d_types[0]
            if first_type != np.object_ and all(np.dtype(np_type) == first_type for np_type in d_types):
                self.np_types = first_type

                def numpy_blocks():
                    for block in block_gen:
                        yield np.array(block, first_type).transpose()
            else:
                if any(x == np.object_ for x in d_types):
                    d_types = [np.object_] * len(d_types)
                self.np_types = np.dtype(list(zip(column_names, d_types)))

                def numpy_blocks():
                    for block in block_gen:
                        yield np.rec.fromarrays(block, self.np_types)

            self._block_gen = numpy_blocks()
        else:
            self._block_gen = empty_gen()
            self.np_types = None

    def stream_np_blocks(self) -> Iterator[np.array]:
        if not self._in_context:
            logger.warning("Streaming results should be used in a 'with' context to ensure the stream is closed")
        if not self._block_gen:
            raise ProgrammingError('Stream closed')

        block_gen = self._block_gen
        d_types = self.np_types
        first_type = d_types[0]

        if first_type != np.object_ and all(np.dtype(np_type) == first_type for np_type in d_types):
            self.np_types = first_type

            def numpy_blocks():
                for block in block_gen:
                    yield np.array(block, first_type).transpose()
        else:
            if any(x == np.object_ for x in d_types):
                self.np_types = [np.object_] * len(self.np_types)
            self.np_types = np.dtype(list(zip(self.column_names, d_types)))

            def numpy_blocks():
                for block in block_gen:
                    yield np.rec.fromarrays(block, self.np_types)

        self._block_gen = None
        return numpy_blocks()

    def stream_pd_blocks(self) -> Iterator[pd.DataFrame]:
        if not self._in_context:
            logger.warning("Streaming results should be used in a 'with' context to ensure the stream is closed")
        if not self._block_gen:
            raise ProgrammingError('Stream closed')
        block_gen = self._block_gen

        def pd_blocks():
            for block in block_gen:
                yield pd.DataFrame.from_dict(dict(zip(self.column_names, block)))

        self._block_gen = None
        return pd_blocks()

    @property
    def np_result(self):
        if self._numpy_result is None:
            if not self._block_gen:
                raise ProgrammingError('Stream closed')
            pieces = [arr for arr in self.stream_np_blocks()]
            self._block_gen = None
            if pieces:
                self._numpy_result = np.concatenate(pieces, dtype=self.np_types)
            else:
                self._numpy_result = []
        return self._numpy_result

    def close(self):
        if self.source:
            self.source.close()
            self.source = None

    def __enter__(self):
        self._in_context = True
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        self._in_context = False
