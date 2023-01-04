# cython: language_level=3
from typing import Union

from cpython.exc cimport PyErr_Occurred
from cpython.mem cimport PyMem_Realloc, PyMem_Free
from libc.string cimport memcpy
from requests import Response


cdef class ResponseBuffer:
    cdef unsigned long long beg, end, buf_loc
    cdef object gen
    cdef unsigned char* buffer

    def __init__(self, response: Response ):
        self.beg = 0
        self.end = 0
        self.buf_loc = 0
        self.gen = response.iter_content(None)
        self.buffer = NULL

    def __getitem__(self, key: Union[slice, int]) -> Union[int, bytes, bytearray, memoryview]:
        if isinstance(key, slice):
            return self.get_slice(key.start, key.stop)
        ret = self.get_index(key)
        if ret == 255 and PyErr_Occurred():
            raise StopIteration
        return ret

    cdef get_slice(self, unsigned long long start, unsigned long long stop):
        cdef unsigned long long x, b, e, rem, ret_loc, cur_len
        cdef char* ptr
        b = self.beg
        e = self.end
        if stop <= e:
            self.buf_loc = stop - b
            return self.buffer[start - b: stop - b]
        cur_len = e - b - self.buf_loc
        ret_buff = bytearray(stop - start)
        if cur_len > 0:
            ret_buff[:cur_len] = self.buffer[self.buf_loc:e - b]
        ret_loc = cur_len
        while True:
            chunk = next(self.gen)
            x = len(chunk)
            if e + x <= stop:
                ret_buff[ret_loc:ret_loc + x] = chunk
                e += x
                if e == stop:
                    self.beg = self.end = e
                    return ret_buff
                ret_loc += x
            else:
                rem = stop - e
                ret_buff[ret_loc:ret_loc + rem] = chunk[:rem]
                self.buffer = <unsigned char *> PyMem_Realloc(self.buffer, x - rem)
                py_chunk = chunk[rem:]
                ptr = <char*>py_chunk
                memcpy(self.buffer, <const void*>ptr, x - rem)
                self.buf_loc = 0
                self.beg = stop
                self.end = e + x
                return ret_buff

    cdef unsigned char get_index(self, unsigned long long ix) except ?255:
        cdef unsigned long long loc, x, b = self.beg, e = self.end
        cdef size_t sz
        cdef char* ptr
        if ix < b:
            raise IndexError
        if ix < e:
            self.buf_loc = ix - b + 1
            return self.buffer[ix - b]
        while True:
            chunk = next(self.gen)
            x = len(chunk)
            if ix < e + x:
                loc = e - ix
                if loc == x - 1:
                    self.beg = self.end = e + x
                else:
                    self.beg = e + loc + 1
                    sz = x - loc - 1
                    self.buffer = <unsigned char*>PyMem_Realloc(self.buffer, sz)
                    py_chunk = chunk[loc + 1:]
                    ptr = <char*>py_chunk
                    memcpy(self.buffer, <const void*>ptr, sz)
                    self.buf_loc = 0
                self.end = e + x
                return chunk[loc]

    def __dealloc__(self):
        PyMem_Free(self.buffer)
