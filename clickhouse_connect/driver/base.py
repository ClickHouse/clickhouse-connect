from abc import ABCMeta, abstractmethod
from typing import List, Any

from clickhouse_connect.datatypes.registry import ClickHouseType


class BaseDriver(metaclass=ABCMeta):

    @abstractmethod
    def query(self, query: str) -> (List[List[Any]], List[str], List[ClickHouseType]):
        pass

    def close(self):
        pass







