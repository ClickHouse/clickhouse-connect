# cython: language_level=3
from cpython cimport Py_INCREF
from cpython.unicode cimport PyUnicode_Decode
from cpython.tuple cimport PyTuple_New, PyTuple_SET_ITEM
from cpython.bytes cimport PyBytes_FromStringAndSize
from cpython.buffer cimport PyObject_GetBuffer, PyBuffer_Release, PyBUF_ANY_CONTIGUOUS, PyBUF_SIMPLE


cdef char * errors = 'strict'


def read_string_column(source, loc: int, num_rows: int, encoding: str):
    """
    Read a column of leb128 encoded strings.  If there is an encoding error the string will be the hex representation
    :param source: Object implementing the Python buffer protocol (so we can get a pointer)
    :param loc: Location to start reading the buffer
    :param num_rows: Expected number of rows/strings to read
    :param encoding: Encoding to use when creating Python strings
    :return: tuple of strings and next read location
    """
    column = PyTuple_New(num_rows)  # preallocate the tuple of strings
    temp_encoding = encoding.encode()
    cdef:
        unsigned long long sz = 0, shift = 0, end = 0, x = 0, cloc = loc, rows = num_rows
        Py_buffer source_buffer
        char * c_encoding = temp_encoding
        char * source_ptr = NULL
        unsigned char b
    PyObject_GetBuffer(source.obj, &source_buffer, PyBUF_SIMPLE | PyBUF_ANY_CONTIGUOUS)
    source_ptr = <char *> source_buffer.buf  # fixed pointer to the beginning of the buffer
    try:
        for x in range(rows):
            sz = 0
            shift = 0
            while 1:
                b = source_ptr[cloc]
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
            Py_INCREF(v)  # Increment the reference count for the new Python string stored in the tuple
            cloc += sz
    finally:
        PyBuffer_Release(&source_buffer)
    return column, cloc


def read_fixed_string_str(source, loc: int, num_rows: int, size: int, encoding: str):
    """
    Read a column of ClickHouse FixedStrings and interpret as a Python string
    :param source: Object implementing the Python buffer protocol (so we can get a pointer)
    :param loc: Location to start reading the buffer
    :param num_rows: Expected number of rows/strings to read
    :param size: Fixed string size
    :param encoding: Encoding to use when creating Python strings
    :return: tuple of strings and next read location
    """
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
    """
    Read a column of ClickHouse FixedStrings as Python bytes objects
    :param source: Object implementing the Python buffer protocol (so we can get a pointer)
    :param loc: Location to start reading the buffer
    :param num_rows: Expected number of rows/strings to read
    :param size: Fixed String/bytes size
    :return: tuple of Python bytes objects and next read location
    """

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
