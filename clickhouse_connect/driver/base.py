from abc import ABCMeta, abstractmethod
from typing import  Any

from clickhouse_connect.driver.query import QueryResult, DataInsert


class BaseDriver(metaclass=ABCMeta):

    def __enter__(self):
        return self

    def __exit__(self):
        self.close()

    @abstractmethod
    def query(self, query: str) -> QueryResult:
        pass

    @abstractmethod
    def insert(self, insert:DataInsert) -> None:
        pass

    @abstractmethod
    def raw_request(self, data=None, **kwargs) -> Any:
        pass

    @abstractmethod
    def command(self, cmd:str) -> str:
        pass

    @abstractmethod
    def ping(self):
        pass

    def close(self):
        pass







