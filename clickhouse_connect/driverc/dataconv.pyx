import struct
from datetime import datetime, date

import cython
from .buffer cimport ResponseBuffer
from cpython cimport Py_INCREF, Py_DECREF
from cpython.mem cimport PyMem_Free, PyMem_Malloc
from cpython.tuple cimport PyTuple_New, PyTuple_SET_ITEM
from cython.view cimport array as cvarray
from ipaddress import IPv4Address
from uuid import UUID, SafeUUID
from libc.string cimport memcpy


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


@cython.boundscheck(False)
@cython.wraparound(False)
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


@cython.boundscheck(False)
@cython.wraparound(False)
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

# Constants used in epoch_days_to_date
# 47482 -- Jan 1, 2100 -- Because all years 1970-2099 divisible by 4 are leap years, some extra division can be avoided
# 134774 -- Number of days between Jan 1 1601 and Jan 1 1970.  Adding this starts all calculations at 1601-01-01
# 1461 -- Number of days in a 4-year cycle (365 * 4) + 1 leap day
# 36524 -- Number of days in a 100-year cycle.  25 4-year cycles - 1 leap day for the year 100
# 146097 -- Number of days in a 400-year cycle.  4 100 year cycles + 1 leap day for the year 400

# Year and offset with in the year are determined by factoring out the largest "known" year blocks in
# descending order (400/100/4/1 years).  Month is then (over) estimated in the "day" arrays (days / 32) and
# adjusted down if too large (logic originally in the Python standard library)

@cython.cdivision(True)
@cython.boundscheck(False)
@cython.wraparound(False)
cpdef inline object epoch_days_to_date(int days):
    cdef int years, month, year, cycles400, cycles100, cycles, rem
    cdef unsigned short prev
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
        if years == 4 or cycles100 == 4:
            return date(year - 1, 12, 31)
        if years == 3 and (year == 2000 or year % 100 != 0):
            m_list = MONTH_DAYS_LEAP
        else:
            m_list = MONTH_DAYS
    month = (rem + 24) >> 5
    prev = m_list[month]
    while rem < prev:
        month -= 1
        prev = m_list[month]
    return date(year, month + 1, rem + 1 - prev)


@cython.boundscheck(False)
@cython.wraparound(False)
def read_uuid_col(ResponseBuffer buffer, unsigned long long num_rows):
    cdef unsigned long long x = 0
    cdef char * loc = buffer.read_bytes_c(16 * num_rows)
    cdef char[16] temp
    cdef object column = PyTuple_New(num_rows), v
    new_uuid = UUID.__new__
    unsafe = SafeUUID.unsafe
    oset = object.__setattr__
    for x in range(num_rows):
        memcpy (<void *>temp, <void *>(loc + 8), 8)
        memcpy (<void *>(temp + 8), <void *>loc, 8)
        v = new_uuid(UUID)
        oset(v, 'int', int.from_bytes(temp[:16], 'little'))
        oset(v, 'is_safe', unsafe)
        PyTuple_SET_ITEM(column, x, v)
        Py_INCREF(v)
        loc += 16
    return column


@cython.boundscheck(False)
@cython.wraparound(False)
def read_nullable_array(ResponseBuffer buffer, array_type: str, unsigned long long num_rows):
    if num_rows == 0:
        return ()
    cdef unsigned long long x = 0

    # We have to make a copy of the incoming null map because the next
    # "read_byes_c" call could invalidate our pointer by replacing the underlying buffer
    cdef char * null_map = <char *>PyMem_Malloc(<size_t>num_rows)
    memcpy(<void *>null_map, <void *>buffer.read_bytes_c(num_rows), num_rows)

    cdef size_t item_size = struct.calcsize(array_type)
    cdef cvarray cy_array = cvarray((num_rows,), item_size, array_type, mode='c', allocate_buffer=False)
    cy_array.data = buffer.read_bytes_c(num_rows * item_size)
    cdef object column = tuple(memoryview(cy_array))
    for x in range(num_rows):
        if null_map[x] != 0:
            Py_DECREF(column[x])
            Py_INCREF(None)
            PyTuple_SET_ITEM(column, x, None)
    PyMem_Free(<void *>null_map)
    return column
