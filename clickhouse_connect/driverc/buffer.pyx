from typing import Union, Generator

from cpython.exc cimport PyErr_Occurred
from cpython.mem cimport PyMem_Realloc, PyMem_Free, PyMem_Malloc
from libc.string cimport memcpy


cdef class ResponseBuffer:
    def __init__(self, gen: Generator[bytes, None, None], buf_size: int = 512 * 1024):
        self.gen = gen
        self.buffer = PyMem_Malloc(buf_size)
        self.slice = PyMem_Malloc(self.slice_sz)

    cdef char set_slice(self, unsigned long long sz) except -1:
        cdef unsigned long long x, e, tail, cur_len, temp
        cdef char* ptr
        e = self.end
        if self.buf_loc + sz <= e:
            self.slice_start = self.buf_loc
            self.buf_loc += sz
            return 0
        self.slice_start = -1
        cur_len = e - self.buf_loc
        temp = self.slice_sz
        while temp < sz * 2:
            temp <<= 1
        if temp > self.slice_sz:
            PyMem_Realloc(self.slice, temp)
            self.slice_sz = temp
        if cur_len > 0:
            memcpy(self.slice, self.buffer + self.buf_loc, cur_len)
        self.buf_loc = 0
        while cur_len < sz:
            chunk = next(self.gen)
            x = len(chunk)
            ptr = <char *> chunk
            if cur_len + x <= sz:
                memcpy(self.slice + cur_len, ptr, x)
                cur_len += x
                if cur_len == sz:
                    self.end = 0
            else:
                tail = sz - cur_len
                memcpy(self.slice + cur_len, ptr, tail)
                memcpy(self.buffer, ptr + tail, x - tail)
                self.end = x - tail
                cur_len += tail


    cdef unsigned char _read_byte(self) except ?255:
        cdef unsigned char ret
        if self.buf_loc < self.end:
            self.buf_loc += 1
            return self.buffer[self.buf_loc - 1]
        self.buf_loc = 0
        self.end = 0
        chunk = next(self.gen)
        x = len(chunk)
        if x == 0:
            raise IndexError
        ret = <unsigned char>chunk[0]
        if x > 1:
            py_chunk = chunk[1:]
            ptr = <char*>py_chunk
            memcpy(self.buffer, <const void*>ptr, x - 1)
            self.end = x - 1
        return ret

    def __dealloc__(self):
        PyMem_Free(self.buffer)
