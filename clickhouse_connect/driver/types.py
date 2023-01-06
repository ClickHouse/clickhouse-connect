from abc import ABC, abstractmethod
from typing import Sequence, Any

Matrix = Sequence[Sequence[Any]]


class ByteSource(ABC):

    @abstractmethod
    def read_leb128(self) -> int:
        pass

    @abstractmethod
    def read_leb128_str(self, encoding: str = 'utf-8') -> str:
        pass

    @abstractmethod
    def read_uint64(self) -> int:
        pass

    @abstractmethod
    def read_bytes(self, sz: int) -> bytes:
        pass

    @abstractmethod
    def read_str_col(self, num_rows: int, encoding: str):
        pass

    @abstractmethod
    def read_array(self, array_type: str, num_rows: int):
        pass

    @abstractmethod
    def read_byte(self) -> int:
        pass
