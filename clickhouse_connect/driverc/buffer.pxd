from cpython cimport PyObject

cdef class ResponseBuffer:
    cdef:
        unsigned long long buf_loc, end, slice_sz
        signed long long slice_start
        object gen
        unsigned char* buffer
        unsigned char* slice
        char _set_slice(self, unsigned long long sz) except 1
        unsigned char* _cur_slice(self)
        unsigned char _read_byte(self) except ?255
        PyObject* read_string(self)

    cpdef unsigned long long read_uint64(self)
    cpdef unsigned long long read_leb128(self)
    cpdef unsigned char read_byte(self)