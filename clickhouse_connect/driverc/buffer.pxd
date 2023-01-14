cdef class ResponseBuffer:
    cdef:
        unsigned long long buf_loc, buf_sz, slice_sz
        signed long long slice_start
        object gen, source
        char* buffer
        char* slice
        unsigned char _read_byte(self) except? 255
        char* _read_bytes(self, unsigned long long sz) except NULL
        Py_buffer buff_source
        cdef object _read_str_col(self, unsigned long long num_rows, char * encoding = *)

