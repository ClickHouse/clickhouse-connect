from cpython cimport PyObject

cdef class ResponseBuffer:
    cdef:
        unsigned long long buf_loc = 0, end = 0, slice_sz = 512
        signed long long slice_start = -1
        object gen
        unsigned char* buffer
        unsigned char* slice = NULL
        unsigned char* read_bytes(self, unsigned long long sz) except NULL
        unsigned char _read_byte(self) except ?255
        PyObject* read_string(self)