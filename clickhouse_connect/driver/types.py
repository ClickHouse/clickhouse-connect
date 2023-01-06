from abc import ABC, abstractmethod
from typing import Union


class ByteSource(ABC):

    @abstractmethod
    def read_leb128(self) -> int:
        pass

    @abstractmethod
    def read_leb128_str(self, encoding: bytes = 'utf-8'.encode()) -> str:
        pass

    @abstractmethod
    def read_uint64(self) -> int:
        pass

    @abstractmethod
    def read_bytes(self, sz: int) -> bytes:
        pass

    @abstractmethod
    def read_str_col(self, num_rows: int, encoding: bytes = 'utf-8'.encode()):
        pass

    @abstractmethod
    def read_array(self, array_type: str, num_rows: int):
        pass
