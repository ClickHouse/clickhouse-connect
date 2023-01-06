
from abc import ABC, abstractmethod


from clickhouse_connect.driver.types import ByteSource

from clickhouse_connect.driver.insert import InsertContext
from clickhouse_connect.driver.query import QueryResult, QueryContext

_EMPTY_QUERY_CONTEXT = QueryContext()


class DataTransform(ABC):

    def parse_response(self, source: ByteSource, context: QueryContext = _EMPTY_QUERY_CONTEXT) -> QueryResult:
        """
        Decodes the ClickHouse byte buffer response into rows of native Python data
        :param source: A byte buffer or similar source
        :param context: The QueryContext to use in processing the response
        :return: QueryResult -- data matrix, column names, column types, etc
        """
        with context:
            return self._transform_response(source, context)

    def build_insert(self, context: InsertContext):
        """
        Encodes a dataset of Python sequences into a ClickHouse format using the specified insert context
        :param context InsertContext parameter object
        :return: generator of bytes like objects containing the dataset in the appropriate format
        """
        with context:
            return self._build_insert(context)

    @abstractmethod
    def _transform_response(self, source: ByteSource, context: QueryContext) -> QueryResult:
        pass

    @abstractmethod
    def _build_insert(self, context: InsertContext):
        pass
