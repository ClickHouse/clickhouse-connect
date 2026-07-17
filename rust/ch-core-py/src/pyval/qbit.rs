use super::*;

fn qbit_shape_error() -> PyErr {
    PyValueError::new_err("Malformed payload: invalid QBit child buffer")
}

/// Build one logical QBit vector as a Python list from its row-major child
/// slice. The core already performs the Native bit-plane transpose once; this
/// exit only allocates the Python objects requested by a row-oriented consumer.
///
/// # Safety
///
/// Requires the GIL. `make` must return a new reference or null. On success the
/// returned pointer is an owned reference that the caller must take over.
unsafe fn float_list_owned_ptr<T, F>(
    py: Python<'_>,
    values: &[T],
    make: F,
) -> PyResult<*mut ffi::PyObject>
where
    T: Copy,
    F: Fn(T) -> *mut ffi::PyObject,
{
    let list_ptr = ffi::PyList_New(values.len() as ffi::Py_ssize_t);
    if list_ptr.is_null() {
        return Err(PyErr::fetch(py));
    }
    // Binding the fresh list makes every error path drop it. CPython list
    // deallocation tolerates the still-null slots after a partial fill.
    let list = Bound::from_owned_ptr(py, list_ptr).downcast_into_unchecked::<PyList>();
    for (index, &value) in values.iter().enumerate() {
        let item = make(value);
        if item.is_null() {
            return Err(PyErr::fetch(py));
        }
        // The fresh list takes ownership of each new float reference.
        ffi::PyList_SET_ITEM(list.as_ptr(), index as ffi::Py_ssize_t, item);
    }
    Ok(list.into_ptr())
}

/// Materialize one non-null QBit row as an owned Python list.
///
/// # Safety
///
/// Requires the GIL. The returned pointer is an owned reference that the
/// caller must take over.
pub(super) unsafe fn qbit_value_owned_ptr(
    py: Python<'_>,
    col: &QBitColumn,
    index: usize,
) -> PyResult<*mut ffi::PyObject> {
    if col.dimension == 0 {
        return Err(qbit_shape_error());
    }
    let start = index
        .checked_mul(col.dimension)
        .ok_or_else(qbit_shape_error)?;
    let end = start
        .checked_add(col.dimension)
        .ok_or_else(qbit_shape_error)?;
    match col.values.as_ref() {
        Column::BFloat16(values) if values.validity.is_none() && end <= values.values.len() => {
            float_list_owned_ptr(py, &values.values[start..end], |value| {
                ffi::PyFloat_FromDouble(bfloat16_to_f32(value).into())
            })
        }
        Column::Float32(values) if values.validity.is_none() && end <= values.values.len() => {
            float_list_owned_ptr(py, &values.values[start..end], |value| {
                ffi::PyFloat_FromDouble(value.into())
            })
        }
        Column::Float64(values) if values.validity.is_none() && end <= values.values.len() => {
            float_list_owned_ptr(py, &values.values[start..end], |value| {
                ffi::PyFloat_FromDouble(value)
            })
        }
        _ => Err(qbit_shape_error()),
    }
}

/// Bulk QBit fill for top-level columns and Tuple/Map field runs. Dispatch the
/// column once, then allocate one Python list per valid row. Nullable QBit has
/// list-level validity, so null rows avoid all child float allocation.
///
/// # Safety
///
/// Requires the GIL. Each pointer passed to `sink` is an owned reference the
/// sink must take over exactly once.
pub(super) unsafe fn fill_qbit<S>(
    py: Python<'_>,
    col: &QBitColumn,
    rows: usize,
    sink: &mut S,
) -> PyResult<()>
where
    S: FnMut(usize, *mut ffi::PyObject),
{
    if col.len() < rows {
        return Err(qbit_shape_error());
    }
    match &col.validity {
        None => {
            for row in 0..rows {
                sink(row, qbit_value_owned_ptr(py, col, row)?);
            }
        }
        Some(validity) => {
            for row in 0..rows {
                let item = if validity.is_valid(row) {
                    qbit_value_owned_ptr(py, col, row)?
                } else {
                    none_owned_ptr()
                };
                sink(row, item);
            }
        }
    }
    Ok(())
}
