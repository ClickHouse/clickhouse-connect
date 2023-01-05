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
