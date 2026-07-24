use super::*;

/// Insert one materialized value at a prepared dotted JSON path. Intermediate
/// dicts are allocated only on first use and then reused by later paths in the
/// same row.
///
/// # Safety
///
/// `root` must be a live exact dict and `value` a live Python object. The GIL
/// must be held.
unsafe fn set_json_path(
    py: Python<'_>,
    root: *mut ffi::PyObject,
    path: &JsonPath<'_>,
    value: *mut ffi::PyObject,
) -> PyResult<()> {
    let Some((last, parents)) = path.keys.split_last() else {
        return Err(json_shape_err());
    };
    let mut current = root;
    for key in parents {
        let mut child = ffi::PyDict_GetItemWithError(current, key.as_ptr());
        if child.is_null() && !ffi::PyErr_Occurred().is_null() {
            return Err(PyErr::fetch(py));
        }
        if child.is_null() || child == ffi::Py_None() {
            let created = ptr_to_result(py, ffi::PyDict_New())?;
            if ffi::PyDict_SetItem(current, key.as_ptr(), created) < 0 {
                ffi::Py_DECREF(created);
                return Err(PyErr::fetch(py));
            }
            // The parent dict owns a reference now; keep only a borrowed
            // pointer while descending.
            ffi::Py_DECREF(created);
            child = created;
        } else if ffi::PyDict_Check(child) == 0 {
            return Err(PyValueError::new_err(
                "Malformed payload: JSON paths collide at a non-object value",
            ));
        }
        current = child;
    }
    if ffi::PyDict_SetItem(current, last.as_ptr(), value) < 0 {
        return Err(PyErr::fetch(py));
    }
    Ok(())
}

/// Fill one JSON path column directly into the already-allocated row dicts.
/// Dynamic/shared NULL values are absent paths, while typed NULL values remain
/// explicit keys.
unsafe fn fill_json_path_column<'py>(
    py: Python<'py>,
    column: &Column,
    ctx: &ColumnCtx<'py>,
    path: &JsonPath<'py>,
    rows: usize,
    containers: &[*mut ffi::PyObject],
    validity: Option<&Bitmap>,
) -> PyResult<()> {
    let mut err: Option<PyErr> = None;
    // Items rejected after the child sink sees them must stay alive until the
    // child fill returns. Some child fills populate containers after sinking.
    let mut discarded: Vec<Bound<'py, PyAny>> = Vec::new();
    let mut path_sink = |row: usize, item: *mut ffi::PyObject| {
        let item = Bound::from_owned_ptr(py, item);
        if err.is_some() || validity.is_some_and(|bitmap| !bitmap.is_valid(row)) {
            discarded.push(item);
            return;
        }
        if let Err(path_err) = set_json_path(py, containers[row], path, item.as_ptr()) {
            err = Some(path_err);
            discarded.push(item);
        }
    };
    let mut erased: DynSink<'_> = &mut path_sink;
    fill_column(py, column, ctx, rows, &mut erased)?;
    if let Some(err) = err {
        return Err(err);
    }
    Ok(())
}

unsafe fn fill_json_dynamic_path<'py>(
    py: Python<'py>,
    column: &DynamicColumn,
    path: &JsonPath<'py>,
    rows: usize,
    containers: &[*mut ffi::PyObject],
    validity: Option<&Bitmap>,
) -> PyResult<()> {
    let mut err: Option<PyErr> = None;
    let mut discarded: Vec<Bound<'py, PyAny>> = Vec::new();
    let mut path_sink = |row: usize, item: *mut ffi::PyObject| {
        let item = Bound::from_owned_ptr(py, item);
        if err.is_some()
            || validity.is_some_and(|bitmap| !bitmap.is_valid(row))
            || item.as_ptr() == ffi::Py_None()
        {
            discarded.push(item);
            return;
        }
        if let Err(path_err) = set_json_path(py, containers[row], path, item.as_ptr()) {
            err = Some(path_err);
            discarded.push(item);
        }
    };
    fill_dynamic(py, column, rows, &mut path_sink)?;
    if let Some(err) = err {
        return Err(err);
    }
    Ok(())
}

/// Materialize a JSON column as nested Python dicts. Typed and dynamic paths
/// are filled column-major, preserving the binding's one-dispatch-per-column
/// policy. Shared-data descriptors and path keys are cached across rows.
///
/// # Safety
///
/// Requires the GIL; `fill_column`'s sink contract applies.
pub(super) unsafe fn fill_json<'py, S>(
    py: Python<'py>,
    col: &JsonColumn,
    ctx: &ColumnCtx<'py>,
    rows: usize,
    sink: &mut S,
) -> PyResult<()>
where
    S: FnMut(usize, *mut ffi::PyObject),
{
    if rows != col.len() {
        return Err(json_shape_err());
    }
    let validity = col.validity.as_ref();
    let JsonBody::Structured(structured) = &col.body else {
        let JsonBody::Text(values) = &col.body else {
            unreachable!()
        };
        let loads = py.import("json")?.getattr("loads")?;
        for row in 0..rows {
            if validity.is_some_and(|bitmap| !bitmap.is_valid(row)) {
                sink(row, none_owned_ptr());
                continue;
            }
            let document = pyo3::types::PyBytes::new(py, values.value(row));
            sink(row, loads.call1((document,))?.into_ptr());
        }
        return Ok(());
    };

    let typed_ctxs = ctx.fields.as_deref().ok_or_else(|| ctx_missing("JSON"))?;
    let typed_paths = ctx
        .json_paths
        .as_deref()
        .ok_or_else(|| ctx_missing("JSON"))?;
    if structured.typed.len() != typed_ctxs.len()
        || structured.typed.len() != typed_paths.len()
        || structured.len != rows
    {
        return Err(json_shape_err());
    }

    let estimated_keys = json_estimated_keys(structured);
    // Borrowed pointers after `sink` takes ownership; the sink contract keeps
    // every valid-row dict alive until this fill returns.
    let mut containers: Vec<*mut ffi::PyObject> = Vec::with_capacity(rows);
    for row in 0..rows {
        let item = if validity.is_some_and(|bitmap| !bitmap.is_valid(row)) {
            none_owned_ptr()
        } else {
            ptr_to_result(py, dict_new_presized(estimated_keys))?
        };
        containers.push(item);
        sink(row, item);
    }

    // The fills below zip the decoded typed columns with the declared-type
    // contexts positionally; guard the order once per column per chunk.
    check_typed_path_order(&structured.typed, typed_paths)?;
    for (((_, column), child_ctx), path) in structured.typed.iter().zip(typed_ctxs).zip(typed_paths)
    {
        fill_json_path_column(py, column, child_ctx, path, rows, &containers, validity)?;
    }

    for (path_name, dynamic) in &structured.dynamic {
        let path = prepare_json_path(py, path_name)?;
        fill_json_dynamic_path(py, dynamic, &path, rows, &containers, validity)?;
    }

    if structured.shared_offsets.len() != rows.saturating_add(1)
        || structured.shared_paths.len() != structured.shared_values.len()
    {
        return Err(json_shape_err());
    }
    let mut path_cache: JsonPathCache<'py> = Vec::new();
    let mut value_ctx_cache: SharedCtxCache<'py> = Vec::new();
    for (row, &container) in containers.iter().enumerate() {
        let start =
            usize::try_from(structured.shared_offsets[row]).map_err(|_| json_shape_err())?;
        let end =
            usize::try_from(structured.shared_offsets[row + 1]).map_err(|_| json_shape_err())?;
        if start > end || end > structured.shared_paths.len() {
            return Err(json_shape_err());
        }
        if validity.is_some_and(|bitmap| !bitmap.is_valid(row)) {
            continue;
        }
        for index in start..end {
            let path = cached_json_path(py, &mut path_cache, structured.shared_paths.value(index))?;
            let item = Bound::from_owned_ptr(
                py,
                shared_cell_owned_ptr(
                    py,
                    structured.shared_values.value(index),
                    Some(&mut value_ctx_cache),
                )?,
            );
            // JSON nulls are absent paths, matching the Python codec.
            if item.as_ptr() != ffi::Py_None() {
                set_json_path(py, container, path, item.as_ptr())?;
            }
        }
    }
    Ok(())
}

/// Row-dict presize estimate: typed/dynamic dotted paths collapse into their
/// distinct top-level keys, shared pairs contribute their per-row mean.
fn json_estimated_keys(structured: &StructuredJson) -> ffi::Py_ssize_t {
    let mean_shared = if structured.len == 0 {
        0
    } else {
        structured.shared_paths.len().div_ceil(structured.len)
    };
    distinct_top_level_keys(structured)
        .saturating_add(mean_shared)
        .min(ffi::Py_ssize_t::MAX as usize) as ffi::Py_ssize_t
}

/// Count the distinct top-level key segments across the typed and dynamic
/// paths, for row-dict presizing.
fn distinct_top_level_keys(structured: &StructuredJson) -> usize {
    let mut seen: Vec<&str> = Vec::new();
    let paths = structured
        .typed
        .iter()
        .map(|(path, _)| path.as_str())
        .chain(structured.dynamic.iter().map(|(path, _)| path.as_str()));
    for path in paths {
        let top = path.split('.').next().unwrap_or(path);
        if !seen.contains(&top) {
            seen.push(top);
        }
    }
    seen.len()
}

/// Guard for the positional zip of decoded typed columns with declared-type
/// contexts: each column's path must equal the declared path at its index.
/// The caller has already checked the lengths match.
fn check_typed_path_order(typed: &[(String, Column)], paths: &[JsonPath<'_>]) -> PyResult<()> {
    for ((declared, _), path) in typed.iter().zip(paths) {
        if declared != &path.raw {
            return Err(PyValueError::new_err(format!(
                "Malformed payload: JSON typed path '{declared}' does not match the declared path '{}'",
                path.raw
            )));
        }
    }
    Ok(())
}

/// Tiny per-fill cache of prepared JSON paths keyed by the raw path bytes.
/// Unique dynamic/shared paths per block are typically single digits, so a
/// linear scan beats hashing.
pub(super) type JsonPathCache<'py> = Vec<(Vec<u8>, JsonPath<'py>)>;

/// Look up or build the prepared path for `path_bytes`.
fn cached_json_path<'cache, 'py>(
    py: Python<'py>,
    cache: &'cache mut JsonPathCache<'py>,
    path_bytes: &[u8],
) -> PyResult<&'cache JsonPath<'py>> {
    match cache.iter().position(|(key, _)| key == path_bytes) {
        Some(found) => Ok(&cache[found].1),
        None => {
            let path_name = std::str::from_utf8(path_bytes).map_err(|_| json_shape_err())?;
            let path = prepare_json_path(py, path_name)?;
            cache.push((path_bytes.to_vec(), path));
            Ok(&cache.last().expect("pushed above").1)
        }
    }
}

/// Materialize one non-null JSON row for recursive container exits. The common
/// top-level path uses `fill_json`'s column-major implementation instead.
/// `cache` is the Array element chain's per-fill `ChainCache::Json`, carrying
/// the resolved `json.loads`, prepared paths, and shared-cell contexts across
/// cells of the same column.
pub(super) unsafe fn json_value_owned_ptr<'py>(
    py: Python<'py>,
    col: &JsonColumn,
    ctx: &ColumnCtx<'py>,
    row: usize,
    cache: Option<&mut ChainCache<'py>>,
) -> PyResult<*mut ffi::PyObject> {
    if row >= col.len() {
        return Err(json_shape_err());
    }
    let json_cache = match cache {
        Some(ChainCache::Json(json_cache)) => Some(json_cache),
        _ => None,
    };
    match &col.body {
        JsonBody::Text(values) => {
            let document = pyo3::types::PyBytes::new(py, values.value(row));
            match json_cache {
                Some(json_cache) => {
                    if json_cache.loads.is_none() {
                        json_cache.loads = Some(py.import("json")?.getattr("loads")?);
                    }
                    let loads = json_cache.loads.as_ref().expect("filled above");
                    Ok(loads.call1((document,))?.into_ptr())
                }
                None => {
                    let loads = py.import("json")?.getattr("loads")?;
                    Ok(loads.call1((document,))?.into_ptr())
                }
            }
        }
        JsonBody::Structured(structured) => {
            let typed_ctxs = ctx.fields.as_deref().ok_or_else(|| ctx_missing("JSON"))?;
            let typed_paths = ctx
                .json_paths
                .as_deref()
                .ok_or_else(|| ctx_missing("JSON"))?;
            if structured.typed.len() != typed_ctxs.len()
                || structured.typed.len() != typed_paths.len()
            {
                return Err(json_shape_err());
            }
            let (mut cached_paths, cached_shared, order_checked, cached_estimate) = match json_cache
            {
                Some(json_cache) => (
                    Some(&mut json_cache.paths),
                    Some(&mut json_cache.shared),
                    Some(&mut json_cache.typed_order_checked),
                    Some(&mut json_cache.estimated_keys),
                ),
                None => (None, None, None, None),
            };
            // Once per column when cached, once per cell otherwise.
            if !order_checked.as_ref().is_some_and(|flag| **flag) {
                check_typed_path_order(&structured.typed, typed_paths)?;
                if let Some(flag) = order_checked {
                    *flag = true;
                }
            }
            let estimated = match cached_estimate {
                Some(slot) => *slot.get_or_insert_with(|| json_estimated_keys(structured)),
                None => json_estimated_keys(structured),
            };
            let root = ptr_to_result(py, dict_new_presized(estimated))?;
            // Own the dict until the row is complete, so every error path drops
            // partially inserted values.
            let root = Bound::from_owned_ptr(py, root).downcast_into_unchecked::<PyDict>();
            for (((_, column), child_ctx), path) in
                structured.typed.iter().zip(typed_ctxs).zip(typed_paths)
            {
                let item = Bound::from_owned_ptr(
                    py,
                    column_value_to_owned_ptr(py, column, child_ctx, row, None)?,
                );
                set_json_path(py, root.as_ptr(), path, item.as_ptr())?;
            }
            for (path_name, dynamic) in &structured.dynamic {
                let item =
                    Bound::from_owned_ptr(py, dynamic_value_owned_ptr(py, dynamic, row, None)?);
                if item.as_ptr() != ffi::Py_None() {
                    let prepared;
                    let path = match cached_paths.as_mut() {
                        Some(cache) => cached_json_path(py, cache, path_name.as_bytes())?,
                        None => {
                            prepared = prepare_json_path(py, path_name)?;
                            &prepared
                        }
                    };
                    set_json_path(py, root.as_ptr(), path, item.as_ptr())?;
                }
            }
            if structured.shared_offsets.len() <= row + 1 {
                return Err(json_shape_err());
            }
            let start =
                usize::try_from(structured.shared_offsets[row]).map_err(|_| json_shape_err())?;
            let end = usize::try_from(structured.shared_offsets[row + 1])
                .map_err(|_| json_shape_err())?;
            if start > end
                || end > structured.shared_paths.len()
                || end > structured.shared_values.len()
            {
                return Err(json_shape_err());
            }
            let mut local_shared: SharedCtxCache<'py> = Vec::new();
            let shared_cache = cached_shared.unwrap_or(&mut local_shared);
            for index in start..end {
                let path_bytes = structured.shared_paths.value(index);
                let prepared;
                let path = match cached_paths.as_mut() {
                    Some(cache) => cached_json_path(py, cache, path_bytes)?,
                    None => {
                        let path_name =
                            std::str::from_utf8(path_bytes).map_err(|_| json_shape_err())?;
                        prepared = prepare_json_path(py, path_name)?;
                        &prepared
                    }
                };
                let item = Bound::from_owned_ptr(
                    py,
                    shared_cell_owned_ptr(
                        py,
                        structured.shared_values.value(index),
                        Some(&mut *shared_cache),
                    )?,
                );
                if item.as_ptr() != ffi::Py_None() {
                    set_json_path(py, root.as_ptr(), path, item.as_ptr())?;
                }
            }
            Ok(root.into_ptr())
        }
    }
}
