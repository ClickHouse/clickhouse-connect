from typing import Union, Generator

import cython

cdef extern from "Python.h":
    PyObject *Py_BuildValue(const char *, ...)

from cpython.exc cimport PyErr_Occurred
from cpython.mem cimport PyMem_Realloc, PyMem_Free, PyMem_Malloc
from libc.string cimport memcpy

cdef union ull_wrapper:
    unsigned char* source
    unsigned long long int_value

cdef class ResponseBuffer:
    def __init__(self, gen: Generator[bytes, None, None], buf_size: int = 512 * 1024):
        self.slice_sz = 512
        self.slice_start = -1
        self.buf_loc = 0
        self.end = 0
        self.gen = gen
        self.buffer = <unsigned char*>PyMem_Malloc(buf_size)
        self.slice = <unsigned char*>PyMem_Malloc(self.slice_sz)

    @cython.inline
    cdef unsigned char* _cur_slice(self):
        if self.slice_start == -1:
            return self.slice
        return self.buffer + self.slice_start

    cdef char _set_slice(self, unsigned long long sz) except 1:
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


    @cython.inline
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
            py_chunk = chunk
            ptr = <char*>py_chunk
            memcpy(self.buffer, <const void*>ptr + 1, x - 1)
            self.end = x - 1
        return ret

    cpdef unsigned char read_byte(self):
        cdef unsigned char ret = self._read_byte()
        if ret == 255 and PyErr_Occurred():
            raise StopIteration
        return ret

    cpdef unsigned long long read_leb128(self):
        cdef:
            unsigned long long sz = 0, shift = 0
            unsigned char b
        while 1:
            b = self._read_byte()
            if b == 255 and PyErr_Occurred():
                raise StopIteration
            sz += ((b & 0x7f) << shift)
            if (b & 0x80) == 0:
                return sz
            shift += 7

    cdef PyObject * read_string(self):
        return NULL

    cpdef unsigned long long read_uint64(self):
        cdef ull_wrapper* x
        if self._set_slice(8) == 1:
            raise IndexError
        x = <ull_wrapper*>self._cur_slice()
        return x.int_value

    def __dealloc__(self):
        PyMem_Free(self.buffer)
