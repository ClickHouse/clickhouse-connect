from datetime import datetime, date

import cython
from .buffer cimport ResponseBuffer
from cpython cimport Py_INCREF
from cpython.tuple cimport PyTuple_New, PyTuple_SET_ITEM
from ipaddress import IPv4Address

@cython.wraparound(False)
@cython.boundscheck(False)
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


@cython.boundscheck(False)
@cython.wraparound(False)
def read_datetime_col(ResponseBuffer buffer, unsigned long long num_rows):
    cdef unsigned long long x = 0
    cdef char * loc = buffer.read_bytes_c(4 * num_rows)
    cdef object column = PyTuple_New(num_rows), v
    fts = datetime.utcfromtimestamp
    while x < num_rows:
        v = fts((<unsigned int*>loc)[0])
        PyTuple_SET_ITEM(column, x, v)
        Py_INCREF(v)
        loc += 4
        x += 1
    return column


def read_date_col(ResponseBuffer buffer, unsigned long long num_rows):
    cdef unsigned long long x = 0
    cdef char * loc = buffer.read_bytes_c(2 * num_rows)
    cdef object column = PyTuple_New(num_rows), v
    while x < num_rows:
        v = epoch_days_to_date((<unsigned short*>loc)[0])
        PyTuple_SET_ITEM(column, x, v)
        Py_INCREF(v)
        loc += 2
        x += 1
    return column


def read_date32_col(ResponseBuffer buffer, unsigned long long num_rows):
    cdef unsigned long long x = 0
    cdef char * loc = buffer.read_bytes_c(4 * num_rows)
    cdef object column = PyTuple_New(num_rows), v
    while x < num_rows:
        v = epoch_days_to_date((<int*>loc)[0])
        PyTuple_SET_ITEM(column, x, v)
        Py_INCREF(v)
        loc += 4
        x += 1
    return column


cdef unsigned short* MONTH_DAYS = [0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334, 365]
cdef unsigned short* MONTH_DAYS_LEAP = [0, 31, 60, 91, 121, 152, 182, 213, 244, 274, 305, 335, 366]


@cython.cdivision(True)
@cython.boundscheck(False)
@cython.wraparound(False)
cpdef inline object epoch_days_to_date(int days):
    cdef int years, month, year, cycles400, cycles100, cycles, rem
    cdef unsigned short* m_list
    if 0 <= days < 47482:
        cycles = (days + 365) // 1461
        rem = (days + 365) - cycles * 1461
        years = rem // 365
        rem -= years * 365
        year = (cycles << 2) + years + 1969
        if years == 4:
            return date(year - 1, 12, 31)
        if years == 3:
            m_list = MONTH_DAYS_LEAP
        else:
            m_list = MONTH_DAYS
    else:
        cycles400 = (days + 134774) // 146097
        rem = days + 134774 - (cycles400 * 146097)
        cycles100 = rem // 36524
        rem -= cycles100 * 36524
        cycles = rem // 1461
        rem -= cycles * 1461
        years = rem // 365
        rem -= years * 365
        year = (cycles << 2) + cycles400 * 400 + cycles100 * 100  + years + 1601
        if years == 4:
            return date(year - 1, 12, 31)
        if years == 3 and (year == 2000 or year % 100 != 0):
            m_list = MONTH_DAYS_LEAP
        else:
            m_list = MONTH_DAYS
    month = (rem + 24) >> 5
    while rem < m_list[month]:
        month -= 1
    return date(year, month + 1, rem + 1 - m_list[month])
