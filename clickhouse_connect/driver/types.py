from abc import ABC, abstractmethod
from typing import Sequence, Any, Protocol

Matrix = Sequence[Sequence[Any]]


class Closable(Protocol):
    def close(self): ...


class ByteSource(ABC, Closable):

    @abstractmethod
    def read_leb128(self) -> int: ...

    @abstractmethod
    def read_leb128_str(self, encoding: str = 'utf-8') -> str: ...

    @abstractmethod
    def read_uint64(self) -> int: ...

    @abstractmethod
    def read_bytes(self, sz: int) -> bytes: ...

    @abstractmethod
    def read_str_col(self, num_rows: int, encoding: str): ...

    @abstractmethod
    def read_array(self, array_type: str, num_rows: int): ...

    @abstractmethod
    def read_byte(self) -> int: ...
