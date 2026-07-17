use super::*;

/// Column-major fill for a Tuple column: allocate every row container up
/// front (tuple, presized dict, or None for a null row), hand each to `sink`,
/// then fill field by field through `fill_column`, one dispatch per field.
///
/// # Safety
///
/// Requires the GIL; `fill_column`'s sink contract applies. Containers are
/// filled after they reach the sink, so the sink keeping items alive until
/// this call returns is load-bearing.
pub(super) unsafe fn fill_tuple<'py>(
    py: Python<'py>,
    c: &TupleColumn,
    ctx: &ColumnCtx<'py>,
    rows: usize,
    sink: DynSink<'_>,
) -> PyResult<()> {
    let fctx = ctx.fields.as_deref().ok_or_else(|| ctx_missing("Tuple"))?;
    if fctx.len() != c.fields.len() {
        return Err(ctx_count_mismatch("Tuple"));
    }
    if c.fields.iter().any(|f| f.len() < rows) {
        return Err(tuple_shape_err());
    }
    let validity = c.validity.as_ref();
    let num_fields = c.fields.len() as ffi::Py_ssize_t;
    let names = ctx.tuple_names.as_deref();

    // Borrowed container pointers; the sink owns them and keeps them alive.
    let mut containers: Vec<*mut ffi::PyObject> = Vec::with_capacity(rows);
    for i in 0..rows {
        let ptr = if validity.is_some_and(|bm| !bm.is_valid(i)) {
            none_owned_ptr()
        } else if names.is_some() {
            ptr_to_result(py, ffi::_PyDict_NewPresized(num_fields))?
        } else {
            ptr_to_result(py, ffi::PyTuple_New(num_fields))?
        };
        containers.push(ptr);
        sink(i, ptr);
    }

    // Items produced for null rows collect here and drop only after the field
    // fill returns, keeping the sink's items-stay-alive contract.
    let mut discarded: Vec<Py<PyAny>> = Vec::new();
    for (field_idx, (field_col, field_ctx)) in c.fields.iter().zip(fctx).enumerate() {
        match names {
            None => {
                let mut field_sink = |i: usize, item: *mut ffi::PyObject| {
                    if validity.is_some_and(|bm| !bm.is_valid(i)) {
                        // Safety: item is an owned reference the sink takes over.
                        discarded.push(unsafe { Py::from_owned_ptr(py, item) });
                        return;
                    }
                    // Safety: containers[i] is a live tuple with num_fields
                    // slots; the tuple takes over the owned item.
                    unsafe {
                        ffi::PyTuple_SET_ITEM(containers[i], field_idx as ffi::Py_ssize_t, item);
                    }
                };
                let mut erased: DynSink<'_> = &mut field_sink;
                fill_column(py, field_col, field_ctx, rows, &mut erased)?;
            }
            Some(names) => {
                let name_ptr = names[field_idx].as_ptr();
                let mut err: Option<PyErr> = None;
                let mut field_sink = |i: usize, item: *mut ffi::PyObject| {
                    // Safety: item is an owned reference the sink takes over.
                    let item = unsafe { Py::<PyAny>::from_owned_ptr(py, item) };
                    if err.is_some() || validity.is_some_and(|bm| !bm.is_valid(i)) {
                        discarded.push(item);
                        return;
                    }
                    // Safety: containers[i] is a live dict and name_ptr a live
                    // str key; SetItem increfs both, our `item` ref drops after.
                    if unsafe { ffi::PyDict_SetItem(containers[i], name_ptr, item.as_ptr()) } < 0 {
                        err = Some(PyErr::fetch(py));
                        discarded.push(item);
                    }
                };
                let mut erased: DynSink<'_> = &mut field_sink;
                fill_column(py, field_col, field_ctx, rows, &mut erased)?;
                if let Some(e) = err {
                    return Err(e);
                }
            }
        }
    }
    Ok(())
}

/// Column-major fill for a Map column: validate the offsets run once,
/// materialize the flat key and value runs through `fill_column`, then zip
/// each row into a presized dict in wire order (last duplicate key wins).
///
/// # Safety
///
/// Requires the GIL; `fill_column`'s sink contract applies.
pub(super) unsafe fn fill_map<'py>(
    py: Python<'py>,
    c: &MapColumn,
    ctx: &ColumnCtx<'py>,
    rows: usize,
    sink: DynSink<'_>,
) -> PyResult<()> {
    if rows == 0 {
        return Ok(());
    }
    let fctx = ctx.fields.as_deref().ok_or_else(|| ctx_missing("Map"))?;
    if fctx.len() != 2 {
        return Err(ctx_missing("Map"));
    }
    let entries = match c.entries.as_ref() {
        Column::Tuple(t) if t.fields.len() == 2 => t,
        _ => return Err(map_entries_err()),
    };
    let keys_col = &entries.fields[0];
    let values_col = &entries.fields[1];
    if c.offsets.len() <= rows {
        return Err(map_bounds_err());
    }
    // One monotonicity pass over the offsets; the per-row casts below are
    // then in range.
    let offsets = &c.offsets[..=rows];
    let mut prev: i64 = 0;
    for &o in offsets {
        if o < prev {
            return Err(map_bounds_err());
        }
        prev = o;
    }
    let total = offsets[rows] as usize;
    if total > keys_col.len().min(values_col.len()) {
        return Err(map_bounds_err());
    }

    let keys = materialize_run(py, keys_col, &fctx[0], total)?;
    let values = materialize_run(py, values_col, &fctx[1], total)?;

    for (i, pair) in offsets.windows(2).enumerate() {
        let (start, end) = (pair[0] as usize, pair[1] as usize);
        let dict_ptr = ffi::_PyDict_NewPresized((end - start) as ffi::Py_ssize_t);
        if dict_ptr.is_null() {
            return Err(PyErr::fetch(py));
        }
        // Safety: dict_ptr came from _PyDict_NewPresized; binding it drops the
        // partially-filled dict on the error path.
        let dict = Bound::from_owned_ptr(py, dict_ptr);
        for slot in start..end {
            // SetItem increfs key and value; the run vectors keep our refs.
            if ffi::PyDict_SetItem(dict.as_ptr(), keys[slot].as_ptr(), values[slot].as_ptr()) < 0 {
                return Err(PyErr::fetch(py));
            }
        }
        sink(i, dict.into_ptr());
    }
    Ok(())
}

/// The two flat Float64 runs of an unnamed non-nullable
/// `Tuple(Float64, Float64)` element column (the geo point shape), when the
/// tight point-list loop applies.
pub(super) fn point_pair_slices<'a>(
    values: &'a Column,
    ctx: &ColumnCtx<'_>,
) -> Option<(&'a [f64], &'a [f64])> {
    let Column::Tuple(t) = values else {
        return None;
    };
    if t.validity.is_some() || ctx.tuple_names.is_some() || t.fields.len() != 2 {
        return None;
    }
    let (Column::Float64(x), Column::Float64(y)) = (&t.fields[0], &t.fields[1]) else {
        return None;
    };
    if x.validity.is_some() || y.validity.is_some() {
        return None;
    }
    Some((&x.values, &y.values))
}

/// Build one Array row's point list from the flat coordinate runs: one
/// presized list, one 2-tuple and two floats per point, with no per-element
/// column dispatch. Allocation stays row-major (tuple, then its floats), the
/// same order as the generic per-cell path; a column-major variant measured
/// faster to fill but slower overall because deallocation and GC traversal of
/// the interleaved result lose locality.
///
/// # Safety
///
/// Requires the GIL. Returns an owned reference the caller takes over.
/// `start..end` must be in range for both slices.
pub(super) unsafe fn point_list_owned_ptr(
    py: Python<'_>,
    xs: &[f64],
    ys: &[f64],
    start: usize,
    end: usize,
) -> PyResult<*mut ffi::PyObject> {
    let count = end - start;
    let list_ptr = ffi::PyList_New(count as ffi::Py_ssize_t);
    if list_ptr.is_null() {
        return Err(PyErr::fetch(py));
    }
    // Safety: list_ptr came from PyList_New, so it is a list and this is the
    // sole owned reference. Binding it makes the error path drop the
    // partially-filled list; list_dealloc tolerates the NULL slots.
    let list = Bound::from_owned_ptr(py, list_ptr).downcast_into_unchecked::<PyList>();
    for (slot, k) in (start..end).enumerate() {
        let tuple_ptr = ffi::PyTuple_New(2);
        if tuple_ptr.is_null() {
            return Err(PyErr::fetch(py));
        }
        // Safety: slot < count, the list's allocated length; the list takes
        // over the owned tuple, so an error below drops it through the list
        // (tuple_dealloc tolerates its NULL slots).
        ffi::PyList_SET_ITEM(list.as_ptr(), slot as ffi::Py_ssize_t, tuple_ptr);
        let x = ptr_to_result(py, ffi::PyFloat_FromDouble(xs[k]))?;
        // Safety: the fresh 2-tuple takes over each owned float.
        ffi::PyTuple_SET_ITEM(tuple_ptr, 0, x);
        let y = ptr_to_result(py, ffi::PyFloat_FromDouble(ys[k]))?;
        ffi::PyTuple_SET_ITEM(tuple_ptr, 1, y);
    }
    Ok(list.into_ptr())
}
