from typing import Generator
from cpython cimport PyObject

import cython

cdef extern from "Python.h":
    PyObject *Py_BuildValue(const char *, ...)

from cpython.unicode cimport PyUnicode_Decode
from cpython.bytes cimport PyBytes_FromStringAndSize
from cpython.exc cimport PyErr_Occurred
from cpython.mem cimport PyMem_Free, PyMem_Malloc
from libc.string cimport memcpy

cdef union ull_wrapper:
    char* source
    unsigned long long int_value

cdef char * errors = 'strict'

cdef class ResponseBuffer:
    def __init__(self, gen: Generator[bytes, None, None], buf_size: int = 1024 * 1024):
        self.slice_sz = 4096
        self.slice_start = -1
        self.buf_loc = 0
        self.end = 0
        self.gen = gen
        self.buffer = <char*>PyMem_Malloc(buf_size)
        self.slice = <char*>PyMem_Malloc(self.slice_sz)

    @cython.inline
    cdef char* _cur_slice(self):
        if self.slice_start == -1:
            return self.slice
        return self.buffer + self.slice_start

    cdef unsigned char _set_slice(self, unsigned long long sz) except 255:
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
            temp <<= 2
        if temp > self.slice_sz:
            PyMem_Free(self.slice)
            self.slice = <char*>PyMem_Malloc(temp)
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
                memcpy(self.buffer, ptr, x)
                self.end = x
                self.buf_loc = tail
                cur_len += tail

    @cython.inline
    cdef unsigned char _read_byte(self) except? 255:
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
        py_chunk = chunk
        ret = <unsigned char>chunk[0]
        if x > 1:
            ptr = <char*>py_chunk
            memcpy(self.buffer, <const void*>ptr + 1, x - 1)
            self.end = x - 1
        return ret

    cdef char* _read_bytes(self, unsigned long long sz) except NULL:
        if self._set_slice(sz) == 255:
            return NULL
        return self._cur_slice()

    def read_leb128_str(self, char* encoding='utf-8') -> str:
        cdef unsigned long long sz = self.read_leb128()
        cdef char* b = self._read_bytes(sz)
        if b == NULL:
            raise StopIteration
        try:
            return PyUnicode_Decode(b, sz, encoding, errors)
        except UnicodeDecodeError:
            return PyBytes_FromStringAndSize(b, sz).hex()

    def read_byte(self) -> int:
        cdef unsigned char ret = self._read_byte()
        if ret == 255 and PyErr_Occurred():
            raise StopIteration
        return ret

    def read_leb128(self) -> int:
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

    def read_uint64(self) -> int:
        cdef ull_wrapper* x
        if self._set_slice(8) == 255:
            raise StopIteration
        x = <ull_wrapper*>self._cur_slice()
        ret = x.int_value
        return x.int_value

    def read_bytes(self, unsigned long long sz):
        if self._set_slice(sz) == 255:
            raise StopIteration
        return self._cur_slice()[:sz]

    def __dealloc__(self):
        PyMem_Free(self.buffer)
        PyMem_Free(self.slice)
