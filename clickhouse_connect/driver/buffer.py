from typing import Union

from requests import Response


class ResponseBuffer:
    def __init__(self, response: Response):
        self.response = response
        self.beg = 0
        self.end = 0
        self.buf_loc = 0
        self.gen = response.iter_content(None)
        self.buffer: bytes = bytes()

    def __getitem__(self, key: Union[slice, int]) -> Union[int, bytes, bytearray, memoryview]:
        if isinstance(key, slice):
            return self.get_slice(key.start, key.stop)
        return self.get_index(key)

    def get_slice(self, start: int, stop: int) -> Union[bytes, bytearray, memoryview]:
        beg = self.beg
        end = self.end
        if stop <= end:
            self.buf_loc = stop - beg
            return memoryview(self.buffer)[start - beg: stop - beg]
        cur_len = end - beg - self.buf_loc
        ret_buff = bytearray(stop - start)
        if cur_len > 0:
            ret_buff[:cur_len] = self.buffer[self.buf_loc:end - beg]
        ret_loc = cur_len
        while True:
            chunk = next(self.gen)
            x = len(chunk)
            if end + x <= stop:
                ret_buff[ret_loc:ret_loc + x] = chunk
                end += x
                if end == stop:
                    self.beg = self.end = end
                    return ret_buff
                ret_loc += x
            else:
                rem = stop - end
                ret_buff[ret_loc:ret_loc + rem] = chunk[:rem]
                self.buffer = chunk[rem:]
                self.buf_loc = 0
                self.beg = stop
                self.end = end + x
                return ret_buff

    def get_index(self, ix):
        beg = self.beg
        end = self.end
        if ix < beg:
            raise IndexError
        if ix < end:
            self.buf_loc = ix - beg + 1
            return self.buffer[ix - beg]
        while True:
            chunk = next(self.gen)
            x = len(chunk)
            if ix < end + x:
                loc = end - ix
                if loc == x - 1:
                    self.beg = self.end = end + x
                else:
                    self.beg = end + loc + 1

                    self.buffer = chunk[loc + 1:]
                    self.buf_loc = 0
                self.end = end + x
                return chunk[loc]
