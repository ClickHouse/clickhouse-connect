import logging
from typing import Generator, Sequence, Tuple, Iterator

from clickhouse_connect.driver.common import empty_gen
from clickhouse_connect.driver.exceptions import ProgrammingError
from clickhouse_connect.driver.types import Closable
from clickhouse_connect.driver.options import np, pd

logger = logging.getLogger(__name__)


# pylint: disable=too-many-instance-attributes
class NumpyResult:
    def __init__(self,
                 block_gen: Generator[Sequence, None, None] = None,
                 column_names: Tuple = (),
                 column_types: Tuple = (),
                 d_types: Sequence = (),
                 source: Closable = None):
        self.column_names = column_names
        self.column_types = column_types
        self.np_types = d_types
        self.source = source
        self.query_id = ''
        self.summary = {}
        self._block_gen = block_gen or empty_gen()
        self._numpy_result = None
        self._pd_result = None
        self._in_context = False

    def stream_np_blocks(self) -> Iterator:
        if not self._in_context:
            logger.warning("Streaming results should be used in a 'with' context to ensure the stream is closed")
        if not self._block_gen:
            raise ProgrammingError('Stream closed')

        block_gen = self._block_gen
        self._block_gen = None
        if not self.np_types:
            return block_gen

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
                    np_array = np.empty(len(block[0]), dtype=self.np_types)
                    for col_name, data in zip(self.column_names, block):
                        np_array[col_name] = data
                    yield np_array

        return numpy_blocks()

    def stream_pd_blocks(self) -> Iterator:
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
            self._in_context = True
            chunk_size = 4
            pieces = []
            blocks = []
            for block in self.stream_np_blocks():
                blocks.append(block)
                if len(blocks) == chunk_size:
                    pieces.append(np.concatenate(blocks, dtype=self.np_types))
                    chunk_size *= 2
                    blocks = []
            pieces.extend(blocks)
            if len(pieces) > 1:
                self._numpy_result = np.concatenate(pieces, dtype=self.np_types)
            elif len(pieces) == 1:
                self._numpy_result = pieces[0]
            else:
                self._numpy_result = np.empty((0,))
            self.close()
        return self._numpy_result

    @property
    def pd_result(self):
        if self._pd_result is None:
            if not self._block_gen:
                raise ProgrammingError('Stream closed')
            self._in_context = True
            pieces = list(self.stream_pd_blocks())
            if len(pieces) > 1:
                self._pd_result = pd.concat(pieces)
            elif len(pieces) == 1:
                self._pd_result = pieces[0]
            else:
                self._pd_result = []
            self.close()
        return self._pd_result

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        self._in_context = False

    def __enter__(self):
        self._in_context = True
        return self

    def close(self):
        self._block_gen = None
        if self.source:
            self.source.close()
            self.source = None
