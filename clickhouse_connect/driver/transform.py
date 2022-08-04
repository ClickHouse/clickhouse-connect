from abc import ABC, abstractmethod
from typing import Sequence, Any

from clickhouse_connect.datatypes.base import ClickHouseType
from clickhouse_connect.driver.query import DataResult, QueryContext


_EMPTY_CONTEXT = QueryContext()


class DataTransform(ABC):

    def parse_response(self, source: Sequence, context: QueryContext = _EMPTY_CONTEXT) -> DataResult:
        """
        Decodes the ClickHouse byte buffer response into rows of native Python data
        :param source: A byte buffer or similar source
        :param context: The QueryContext to use in processing the response
        :return: DataResult -- data matrix, column names, column types
        """
        with context:
            return self._transform_response(source, context)

    @abstractmethod
    def build_insert(self, data: Sequence[Sequence[Any]], *, column_names: Sequence[str],
                     column_type_names: Sequence[str] = None,
                     column_types: Sequence[ClickHouseType] = None,
                     column_oriented: bool = False):
        """
        Encodes a dataset of Python sequences into a ClickHouse format
        :param data: Matrix of rows and columns of data
        :param column_names: Column names of the data to insert
        :param column_type_names: Column type names of the data
        :param column_types: Column types used to encode data in ClickHouse native format
        :param column_oriented: If true the dataset does not need to be "pivoted"
        :return: bytearray containing the dataset in the appropriate format
        """

    @abstractmethod
    def _transform_response(self, source: Sequence, context: QueryContext) -> DataResult:
        pass
