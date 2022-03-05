from abc import ABCMeta, abstractmethod
from typing import  Any

from clickhouse_connect.driver.query import QueryResult


class BaseDriver(metaclass=ABCMeta):

    def __enter__(self):
        return self

    def __exit__(self):
        self.close()

    @abstractmethod
    def query(self, query: str) -> QueryResult:
        pass

    @abstractmethod
    def raw_request(self, data=None, **kwargs) -> Any:
        pass

    def close(self):
        pass







