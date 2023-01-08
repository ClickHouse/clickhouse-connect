import sys

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
cdef dict array_templates = {}
cdef bint must_swap = sys.byteorder == 'big'
cdef array.array swapper = array.array('Q', [0])

for c in 'bBuhHiIlLqQfd':
    array_templates[c] = array.array(c, [])


cdef class ResponseBuffer:
    def __init__(self, source):
        self.slice_sz = 4096
        self.slice_start = -1
        self.buf_loc = 0
        self.end = 0
        self.source = source
        self.gen = source.gen
        self.buffer = NULL
        self.slice = <char*>PyMem_Malloc(self.slice_sz)

    @cython.inline
    cdef char* _cur_slice(self):
        if self.slice_start == -1:
            return self.slice
        return self.buffer + self.slice_start

    @cython.inline
    cdef _reset_buff(self, object source):
        PyBuffer_Release(&self.buff_source)
        PyObject_GetBuffer(source, &self.buff_source, PyBUF_SIMPLE | PyBUF_ANY_CONTIGUOUS)
        self.buffer = <char *> self.buff_source.buf

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
                self._reset_buff(chunk)
                self.end = x
                self.buf_loc = tail
                cur_len += tail

    @cython.inline
    cdef unsigned char _read_byte(self) except? 255:
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
        if x > 1:
            self._reset_buff(chunk)
            self.buf_loc = 1
            self.end = x
        return <unsigned char>chunk[0]

    @cython.inline
    cdef char* _read_bytes(self, unsigned long long sz) except NULL:
        if self._set_slice(sz) == 255:
            return NULL
        return self._cur_slice()

    @cython.inline
    cdef object _read_leb128_str(self, char* encoding='utf8'):
        cdef unsigned long long sz = self.read_leb128()
        cdef char* b = self._read_bytes(sz)
        if b == NULL:
            raise StopIteration
        try:
            return PyUnicode_Decode(b, sz, encoding, errors)
        except UnicodeDecodeError:
            return PyBytes_FromStringAndSize(b, sz).hex()

    def read_leb128_str(self, encoding: str='utf8'):
        return self._read_leb128_str(encoding.encode())

    def read_byte(self) -> int:
        cdef unsigned char ret = self._read_byte()
        if ret == 255 and PyErr_Occurred():
            raise StopIteration
        return ret

    @cython.inline
    cpdef unsigned long long read_leb128(self) except? 99999:
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
        if must_swap:
            memcpy(swapper.data.as_voidptr, self._cur_slice(), 8)
            swapper.byteswap()
            return swapper[0]
        x = <ull_wrapper *> self._cur_slice()
        return x.int_value

    def read_bytes(self, unsigned long long sz):
        if self._set_slice(sz) == 255:
            raise StopIteration
        return self._cur_slice()[:sz]

    def read_str_col(self, unsigned long long num_rows, encoding: str = 'utf8'):
        cdef object column = PyTuple_New(num_rows)
        cdef char* enc
        pyenc = encoding.encode()
        enc = pyenc
        for x in range(num_rows):
            v = self._read_leb128_str(enc)
            PyTuple_SET_ITEM(column, x, v)
            Py_INCREF(v)
        return column

    def read_array(self, t: str, unsigned long long num_rows):
        cdef array.array template = array_templates[t]
        cdef array.array result = array.clone(template, num_rows, 0)
        cdef unsigned long long sz = result.itemsize * num_rows
        if self._set_slice(sz) == 255:
            raise StopIteration
        memcpy(result.data.as_voidptr, self._cur_slice(), sz)
        if must_swap:
            result.byteswap()
        return result

    def read_bytes_col(self, unsigned long long sz, unsigned long long num_rows):
        cdef object column = PyTuple_New(num_rows)
        if self._set_slice(sz * num_rows) == 255:
            raise StopIteration
        cdef char* start = self._cur_slice()
        for x in range(num_rows):
            v = PyBytes_FromStringAndSize(start, sz)
            start += sz
            PyTuple_SET_ITEM(column, x, v)
            Py_INCREF(v)
        return column

    def read_fixed_str_col(self, unsigned long long sz, unsigned long long num_rows,
                           encoding:str ='utf8'):
        cdef object column = PyTuple_New(num_rows)
        cdef char * enc
        cdef object v
        pyenc = encoding.encode()
        enc = pyenc
        if self._set_slice(sz * num_rows) == 255:
            raise StopIteration
        cdef char * start = self._cur_slice()
        for x in range(num_rows):
            try:
                v = PyUnicode_Decode(start, sz, enc, errors)
            except UnicodeDecodeError:
                v = PyBytes_FromStringAndSize(start, sz).hex()
            PyTuple_SET_ITEM(column, x, v)
            Py_INCREF(v)
        return column

    def close(self):
        if self.source:
            self.source.close()
            self.source = None

    def __dealloc__(self):
        self.close()
        PyBuffer_Release(&self.buff_source)
        PyMem_Free(self.slice)
