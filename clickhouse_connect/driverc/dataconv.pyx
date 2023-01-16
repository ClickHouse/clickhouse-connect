from .buffer cimport ResponseBuffer
from cpython cimport Py_INCREF
from cpython.tuple cimport PyTuple_New, PyTuple_SET_ITEM, PyTuple_SetItem
from ipaddress import IPv4Address

def read_ipv4_col(ResponseBuffer buffer, unsigned long long num_rows):
    cdef unsigned long long x = 0
    cdef char* loc = buffer.read_bytes_c(4 * num_rows)
    cdef object column = PyTuple_New(num_rows), v
    ip_new = IPv4Address.__new__
    while x < num_rows:
        v = ip_new(IPv4Address)
        v._ip = (<unsigned int*>loc)[0]
        PyTuple_SET_ITEM(column, x, v)
        Py_INCREF(v)
        loc += 4
        x += 1
    return column