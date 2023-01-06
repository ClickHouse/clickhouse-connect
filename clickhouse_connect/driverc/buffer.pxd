cdef class ResponseBuffer:
    cdef:
        unsigned long long buf_loc, end, slice_sz
        signed long long slice_start
        object gen
        char* buffer
        char* slice
        unsigned char _set_slice(self, unsigned long long sz) except 255
        char* _cur_slice(self)
        unsigned char _read_byte(self) except? 255
        char* _read_bytes(self, unsigned long long sz) except NULL
        _reset_buff(self, object source)
        Py_buffer buff_source

    cpdef object read_leb128_str(self, char * encoding = *)
    cpdef unsigned long long read_leb128(self) except? 99999

