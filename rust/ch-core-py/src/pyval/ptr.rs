use super::*;

/// Build a Python str from raw String column bytes. Invalid UTF-8 renders as
/// the lowercase hex of the raw bytes, matching clickhouse-connect's String
/// read fallback. Single scan in the valid case: CPython's decode is the
/// validation, and the hex path runs only after a UnicodeDecodeError.
pub(super) fn utf8_or_hex_owned_ptr(py: Python<'_>, bytes: &[u8]) -> PyResult<*mut ffi::PyObject> {
    // Safety: the pointer/length pair is a live borrowed slice and CPython
    // copies the bytes before returning. A zero-length slice's dangling
    // pointer is never read.
    let ptr = unsafe {
        ffi::PyUnicode_FromStringAndSize(
            bytes.as_ptr() as *const c_char,
            bytes.len() as ffi::Py_ssize_t,
        )
    };
    if !ptr.is_null() {
        return Ok(ptr);
    }
    let err = PyErr::fetch(py);
    if !err.is_instance_of::<PyUnicodeDecodeError>(py) {
        return Err(err);
    }
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut hex = Vec::with_capacity(bytes.len() * 2);
    for &b in bytes {
        hex.push(HEX[(b >> 4) as usize]);
        hex.push(HEX[(b & 0x0f) as usize]);
    }
    // Safety: hex is a live ASCII buffer, valid UTF-8, copied by CPython.
    unsafe {
        ptr_to_result(
            py,
            ffi::PyUnicode_FromStringAndSize(
                hex.as_ptr() as *const c_char,
                hex.len() as ffi::Py_ssize_t,
            ),
        )
    }
}

/// # Safety
///
/// Returns an owned reference; the caller must take over the reference count.
pub(super) unsafe fn none_owned_ptr() -> *mut ffi::PyObject {
    let none = ffi::Py_None();
    ffi::Py_INCREF(none);
    none
}

/// # Safety
///
/// Returns an owned reference; the caller must take over the reference count.
pub(super) unsafe fn bool_owned_ptr(value: bool) -> *mut ffi::PyObject {
    let ptr = if value {
        ffi::Py_True()
    } else {
        ffi::Py_False()
    };
    ffi::Py_INCREF(ptr);
    ptr
}

/// # Safety
///
/// `ptr` must be an owned reference or null; on `Ok` the caller takes over
/// the reference count.
pub(super) unsafe fn ptr_to_result(
    py: Python<'_>,
    ptr: *mut ffi::PyObject,
) -> PyResult<*mut ffi::PyObject> {
    if ptr.is_null() {
        Err(PyErr::fetch(py))
    } else {
        Ok(ptr)
    }
}

/// Copy one binary value into a Python bytes object.
///
/// # Safety
///
/// Requires the GIL. Returns an owned reference; the caller must take over the
/// reference count.
pub(super) unsafe fn bytes_owned_ptr(py: Python<'_>, bytes: &[u8]) -> PyResult<*mut ffi::PyObject> {
    ptr_to_result(
        py,
        ffi::PyBytes_FromStringAndSize(
            bytes.as_ptr() as *const c_char,
            bytes.len() as ffi::Py_ssize_t,
        ),
    )
}
