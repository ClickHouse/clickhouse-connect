import sys
from typing import Iterable, Any

import cython

from cpython cimport Py_INCREF, array
import array
from cpython.unicode cimport PyUnicode_Decode
from cpython.tuple cimport PyTuple_New, PyTuple_SET_ITEM
from cpython.bytes cimport PyBytes_FromStringAndSize
from cpython.buffer cimport PyObject_GetBuffer, PyBuffer_Release, PyBUF_ANY_CONTIGUOUS, PyBUF_SIMPLE
from cpython.exc cimport PyErr_Occurred
from cpython.mem cimport PyMem_Free, PyMem_Malloc
from libc.string cimport memcpy

cdef union ull_wrapper:
    char* source
    unsigned long long int_value

cdef char * errors = 'strict'
cdef char * utf8 = 'utf8'
cdef dict array_templates = {}
cdef bint must_swap = sys.byteorder == 'big'
cdef array.array swapper = array.array('Q', [0])

for c in 'bBuhHiIlLqQfd':
    array_templates[c] = array.array(c, [])


cdef class ResponseBuffer:
    def __init__(self, source):
        self.slice_sz = 4096
        self.buf_loc = 0
        self.buf_sz = 0
        self.source = source
        self.gen = source.gen
        self.buffer = NULL
        self.slice = <char*>PyMem_Malloc(self.slice_sz)

    @cython.boundscheck(False)
    @cython.wraparound(False)
    cdef inline char * read_bytes_c(self, unsigned long long sz) except NULL:
        cdef unsigned long long x, e, tail, cur_len, temp
        cdef char* ptr
        e = self.buf_sz
        if self.buf_loc + sz <= e:
            temp = self.buf_loc
            self.buf_loc += sz
            return self.buffer + temp
        cur_len = e - self.buf_loc
        temp = self.slice_sz
        while temp < sz * 2:
            temp <<= 1
        if temp > self.slice_sz:
            PyMem_Free(self.slice)
            self.slice = <char*>PyMem_Malloc(temp)
            self.slice_sz = temp
        if cur_len > 0:
            memcpy(self.slice, self.buffer + self.buf_loc, cur_len)
        self.buf_loc = 0
        self.buf_sz = 0
        while cur_len < sz:
            chunk = next(self.gen)
            x = len(chunk)
            ptr = <char *> chunk
            if cur_len + x <= sz:
                memcpy(self.slice + cur_len, ptr, x)
                cur_len += x
            else:
                tail = sz - cur_len
                memcpy(self.slice + cur_len, ptr, tail)
                PyBuffer_Release(&self.buff_source)
                PyObject_GetBuffer(chunk, &self.buff_source, PyBUF_SIMPLE | PyBUF_ANY_CONTIGUOUS)
                self.buffer = <char *> self.buff_source.buf
                self.buf_sz = x
                self.buf_loc = tail
                cur_len += tail
        return self.slice

    @cython.boundscheck(False)
    @cython.wraparound(False)
    cdef inline unsigned char _read_byte_load(self) except? 255:
        self.buf_loc = 0
        self.buf_sz = 0
        chunk = next(self.gen)
        x = len(chunk)
        if x == 0:
            raise IndexError
        py_chunk = chunk
        if x > 1:
            PyBuffer_Release(&self.buff_source)
            PyObject_GetBuffer(chunk, &self.buff_source, PyBUF_SIMPLE | PyBUF_ANY_CONTIGUOUS)
            self.buffer = <char *> self.buff_source.buf
            self.buf_loc = 1
            self.buf_sz = x
        return <unsigned char>chunk[0]

    @cython.boundscheck(False)
    @cython.wraparound(False)
    cdef inline object _read_str_col(self, unsigned long long num_rows, char* encoding = 'utf8'):
        cdef object column = PyTuple_New(num_rows), v
        cdef unsigned long long x = 0, sz, shift
        cdef unsigned char b
        cdef char* buf
        while x < num_rows:
            sz = 0
            shift = 0
            while 1:
                if self.buf_loc < self.buf_sz:
                    b = self.buffer[self.buf_loc]
                    self.buf_loc += 1
                else:
                    b = self._read_byte_load()
                    if b == 255 and PyErr_Occurred():
                        raise StopIteration
                sz += ((b & 0x7f) << shift)
                if (b & 0x80) == 0:
                    break
                shift += 7
            buf = self.read_bytes_c(sz)
            try:
                v = PyUnicode_Decode(buf, sz, encoding, errors)
            except UnicodeDecodeError:
                v = PyBytes_FromStringAndSize(buf, sz).hex()
            PyTuple_SET_ITEM(column, x, v)
            Py_INCREF(v)
            x += 1
        return column

    @cython.boundscheck(False)
    @cython.wraparound(False)
    cpdef unsigned char read_byte(self):
        if self.buf_loc < self.buf_sz:
            b = self.buffer[self.buf_loc]
            self.buf_loc += 1
            return b
        b = self._read_byte_load()
        if b == 255 and PyErr_Occurred():
            raise StopIteration
        return b

    def read_leb128_str(self) -> str:
        cdef unsigned long long sz = self.read_leb128()
        cdef char * b = self.read_bytes_c(sz)
        if b == NULL:
            raise StopIteration
        return PyUnicode_Decode(b, sz, utf8, errors)

    @cython.boundscheck(False)
    @cython.wraparound(False)
    def read_leb128(self) -> int:
        cdef:
            unsigned long long sz = 0, shift = 0
            unsigned char b
        while 1:
            if self.buf_loc < self.buf_sz:
                b = self.buffer[self.buf_loc]
                self.buf_loc += 1
            else:
                b = self._read_byte_load()
                if b == 255 and PyErr_Occurred():
                    raise StopIteration
            sz += ((b & 0x7f) << shift)
            if (b & 0x80) == 0:
                return sz
            shift += 7

    @cython.boundscheck(False)
    @cython.wraparound(False)
    def read_uint64(self) -> int:
        cdef ull_wrapper* x
        cdef char* b = self.read_bytes_c(8)
        if b == NULL:
            raise StopIteration
        if must_swap:
            memcpy(swapper.data.as_voidptr, b, 8)
            swapper.byteswap()
            return swapper[0]
        x = <ull_wrapper *> b
        return x.int_value

    @cython.boundscheck(False)
    @cython.wraparound(False)
    def read_bytes(self, unsigned long long sz) -> bytes:
        cdef char* b = self.read_bytes_c(sz)
        if b == NULL:
            raise StopIteration
        return b[:sz]

    def read_str_col(self, unsigned long long num_rows, encoding: str = 'utf8') -> Iterable[str]:
        return self._read_str_col(num_rows, encoding.encode())

    @cython.boundscheck(False)
    @cython.wraparound(False)
    def read_array(self, t: str, unsigned long long num_rows) -> Iterable[Any]:
        cdef array.array template = array_templates[t]
        cdef array.array result = array.clone(template, num_rows, 0)
        cdef unsigned long long sz = result.itemsize * num_rows
        cdef char * b = self.read_bytes_c(sz)
        if b == NULL:
            raise StopIteration
        memcpy(result.data.as_voidptr, b, sz)
        if must_swap:
            result.byteswap()
        return result

    @cython.boundscheck(False)
    @cython.wraparound(False)
    def read_bytes_col(self, unsigned long long sz, unsigned long long num_rows) -> Iterable[Any]:
        cdef object column = PyTuple_New(num_rows)
        cdef char * b = self.read_bytes_c(sz * num_rows)
        if b == NULL:
            raise StopIteration
        for x in range(num_rows):
            v = PyBytes_FromStringAndSize(b, sz)
            b += sz
            PyTuple_SET_ITEM(column, x, v)
            Py_INCREF(v)
        return column

    @cython.boundscheck(False)
    @cython.wraparound(False)
    def read_fixed_str_col(self, unsigned long long sz, unsigned long long num_rows,
                           encoding:str ='utf8') -> Iterable[str]:
        cdef object column = PyTuple_New(num_rows)
        cdef char * enc
        cdef char * b = self.read_bytes_c(sz * num_rows)
        if b == NULL:
            raise StopIteration
        cdef object v
        pyenc = encoding.encode()
        enc = pyenc
        for x in range(num_rows):
            try:
                v = PyUnicode_Decode(b, sz, enc, errors)
            except UnicodeDecodeError:
                v = PyBytes_FromStringAndSize(b, sz).hex()
            PyTuple_SET_ITEM(column, x, v)
            Py_INCREF(v)
            b += sz
        return column

    def close(self):
        if self.source:
            self.source.close()
            self.source = None

    def __dealloc__(self):
        self.close()
        PyBuffer_Release(&self.buff_source)
        PyMem_Free(self.slice)
