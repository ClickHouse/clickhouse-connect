# cython: language_level=3
from cpython cimport Py_INCREF
from cpython.unicode cimport PyUnicode_Decode
from cpython.tuple cimport PyTuple_New, PyTuple_SET_ITEM
from cpython.bytes cimport PyBytes_FromStringAndSize
from cpython.buffer cimport PyObject_GetBuffer, PyBuffer_Release, PyBUF_ANY_CONTIGUOUS, PyBUF_SIMPLE


cdef char * errors = 'strict'


def read_string_column(source, loc: int, num_rows: int, encoding: str):
    column = PyTuple_New(num_rows)
    temp_encoding = encoding.encode()
    cdef:
        unsigned long long sz = 0, shift = 0, end = 0, x = 0, cloc = loc, rows = num_rows
        Py_buffer source_buffer
        char * c_encoding = temp_encoding
        char * source_ptr = NULL
        unsigned char b
    PyObject_GetBuffer(source.obj, &source_buffer, PyBUF_SIMPLE | PyBUF_ANY_CONTIGUOUS)
    source_ptr = <char *> source_buffer.buf
    try:
        for x in range(rows):
            sz = 0
            shift = 0
            while 1:
                b = source[cloc]
                sz += ((b & 0x7f) << shift)
                cloc += 1
                if (b & 0x80) == 0:
                    break
                shift += 7
            try:
                v = PyUnicode_Decode(&source_ptr[cloc], sz, c_encoding, errors)
            except UnicodeDecodeError:
                v = PyBytes_FromStringAndSize(&source_ptr[cloc], sz).hex()
            PyTuple_SET_ITEM(column, x, v)
            Py_INCREF(v)
            cloc += sz
    finally:
        PyBuffer_Release(&source_buffer)
    return column, cloc


def read_fixed_string_str(source, loc: int, num_rows: int, size: int, encoding: str):
    column = PyTuple_New(num_rows)
    temp_encoding = encoding.encode()
    cdef:
        unsigned long long cloc = loc, x = 0, sz = size, rows = num_rows
        Py_buffer source_buffer
        char * c_encoding = temp_encoding
        char * source_ptr = NULL
    PyObject_GetBuffer(source.obj, &source_buffer, PyBUF_SIMPLE | PyBUF_ANY_CONTIGUOUS)
    source_ptr = <char *> source_buffer.buf
    try:
        for x in range(rows):
            try:
                v = PyUnicode_Decode(&source_ptr[cloc], sz, c_encoding, errors)
            except UnicodeDecodeError:
                v = PyBytes_FromStringAndSize(&source_ptr[cloc], sz).hex()
            PyTuple_SET_ITEM(column, x, v)
            Py_INCREF(v)
            cloc += sz
    finally:
        PyBuffer_Release(&source_buffer)
    return column, cloc


def read_fixed_string_bytes(source, loc: int, num_rows: int, size: int):
    column = PyTuple_New(num_rows)
    cdef:
        unsigned long long cloc = loc, x = 0, sz = size, rows = num_rows
        Py_buffer source_buffer
        char * source_ptr = NULL
    PyObject_GetBuffer(source.obj, &source_buffer, PyBUF_SIMPLE | PyBUF_ANY_CONTIGUOUS)
    source_ptr = <char *> source_buffer.buf
    try:
        for x in range(rows):
            v = PyBytes_FromStringAndSize(&source_ptr[cloc], sz)
            PyTuple_SET_ITEM(column, x, v)
            Py_INCREF(v)
            cloc += sz
    finally:
        PyBuffer_Release(&source_buffer)
    return column, cloc
