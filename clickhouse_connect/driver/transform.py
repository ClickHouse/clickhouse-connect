import threading
from abc import ABC, abstractmethod
from typing import Sequence, Dict, Union, Any, Optional

from clickhouse_connect.datatypes.base import ClickHouseType
from clickhouse_connect.datatypes.format import format_map
from clickhouse_connect.driver.query import DataResult


class QueryContext:
    def __init__(self, use_none: bool, type_formats: Optional[Dict[str, str]],
                 _column_formats: Optional[Dict[str, Union[str, Dict[str, str]]]]):
        self.query_overrides = format_map(type_formats)
        self.use_none = use_none

    def __enter__(self):
        if self.query_overrides:
            threading.local.ch_query_overrides = self.query_overrides
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.query_overrides:
            del threading.local.ch_query_overrides


class DataTransform(ABC):

    def parse_response(self, source: Sequence, type_formats: Dict[str, str] = None, use_none: bool = True,
                       column_formats: Dict[str, Union[str, Dict[str, str]]] = None) -> DataResult:
        """
        Decodes the ClickHouse byte buffer response into rows of native Python data
        :param source: A byte buffer or similar source
        :param use_none: Use None python value for ClickHouse nulls (otherwise use type "zero value")
        :param type_formats:  Dictionary of ClickHouse type names/patterns and response formats
        :param column_formats: Use None values for ClickHouse NULLs (otherwise use zero/empty values)
        :return: DataResult -- data matrix, column names, column types
        """
        with QueryContext(use_none, type_formats, column_formats) as query_context:
            return self._transform_response(source, query_context)

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
