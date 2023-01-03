from abc import ABC, abstractmethod
from typing import Union


class ByteSource(ABC):

    @abstractmethod
    def __getitem__(self, key: Union[slice, int]) -> Union[int, bytes, bytearray, memoryview]:
        pass
