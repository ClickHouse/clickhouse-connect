use super::*;

/// Build an `Array(T)` column: each row is a sequence of elements, flattened
/// into one strong-reference element run with an Arrow LargeList offsets run.
/// The element column is built once over the flat run, so elements hit the
/// same per-type fast paths as a plain column and nested Arrays compose
/// recursively. Arrays are never nullable at the array level, so a None row
/// is an error.
pub(super) fn build_array_column(
    py: Python<'_>,
    name: &str,
    inner: &ChType,
    values: &Bound<'_, PyAny>,
    row_count: usize,
) -> PyResult<Column> {
    let column_values = ColumnValues::new(values, name)?;
    check_row_count(name, &column_values, row_count)?;

    if let Ok(list) = values.downcast_exact::<PyList>() {
        return array_column_from_seq(py, name, inner, &ListSeq(list), row_count);
    }
    if let Ok(tuple) = values.downcast_exact::<PyTuple>() {
        return array_column_from_seq(py, name, inner, &TupleSeq(tuple), row_count);
    }

    // Generic outer container: safe indexed reads, same per-row flatten.
    let mut offsets = Vec::with_capacity(row_count + 1);
    offsets.push(0i64);
    let mut flat = FlatRefs::default();
    for row in 0..row_count {
        let value = column_values.get_item(row)?;
        flatten_array_row(py, name, inner, &value, row, &mut flat)?;
        offsets.push(flat.end_offset(name, "Array element")?);
    }
    let element_column = build_element_column(py, name, inner, &flat.ptrs)
        .map_err(|err| remap_element_err(py, name, &offsets, err))?;
    Ok(Column::Array(ArrayColumn::new(offsets, element_column)))
}

/// Array flatten over an exact list or tuple of rows: borrowed row reads, one
/// flat element run. A row that is not an exact list or tuple flattens through
/// `list.extend`, which can run Python, so the container size is revalidated
/// before the next borrowed read.
fn array_column_from_seq<S: FastSeq>(
    py: Python<'_>,
    name: &str,
    inner: &ChType,
    seq: &S,
    row_count: usize,
) -> PyResult<Column> {
    let mut offsets = Vec::with_capacity(row_count + 1);
    offsets.push(0i64);
    let mut flat = FlatRefs::default();
    for row in 0..row_count {
        // SAFETY: row < row_count, the container size the caller checked and
        // every fallback revalidates; the strong reference keeps the row
        // alive across any Python code the fallback runs.
        let value = unsafe { Bound::from_borrowed_ptr(py, seq.get(row)) };
        flatten_array_row(py, name, inner, &value, row, &mut flat)?;
        check_not_resized(seq, name, row_count)?;
        offsets.push(flat.end_offset(name, "Array element")?);
    }
    let element_column = build_element_column(py, name, inner, &flat.ptrs)
        .map_err(|err| remap_element_err(py, name, &offsets, err))?;
    Ok(Column::Array(ArrayColumn::new(offsets, element_column)))
}

/// Rewrite an element-run error's flat index as the outer row and element
/// index, using the offsets built alongside the flat run. Nested Arrays remap
/// at each level, so the final message leads with the outermost row. An error
/// that is not a ValueError or whose text does not carry the `column "name"
/// row N` prefix passes through unchanged.
fn remap_element_err(py: Python<'_>, name: &str, offsets: &[i64], err: PyErr) -> PyErr {
    remap_flat_err(py, name, offsets, "element", err)
}

/// Shared flat-run error rewrite: `unit` names what the flat index counts
/// ("element" for Array, "key"/"value" for Map).
fn remap_flat_err(py: Python<'_>, name: &str, offsets: &[i64], unit: &str, err: PyErr) -> PyErr {
    let Some((flat, prefix, tail)) = parse_row_err(py, name, &err) else {
        return err;
    };
    let Ok(flat) = i64::try_from(flat) else {
        return err;
    };
    let row = offsets[1..].partition_point(|&end| end <= flat);
    if row + 1 >= offsets.len() {
        return err;
    }
    let element = flat - offsets[row];
    PyValueError::new_err(format!("{prefix}{row} {unit} {element}{tail}"))
}

/// Parse a ValueError's `column {name:?} row N` prefix into the row index,
/// the prefix through "row ", and the message tail after the digits. `None`
/// for any error that does not carry the prefix.
fn parse_row_err(py: Python<'_>, name: &str, err: &PyErr) -> Option<(usize, String, String)> {
    if !err.is_instance_of::<PyValueError>(py) {
        return None;
    }
    let text = err.value(py).str().ok()?;
    let text = text.to_str().ok()?;
    let prefix = format!("column {name:?} row ");
    let rest = text.strip_prefix(&prefix)?;
    let digits = rest.bytes().take_while(u8::is_ascii_digit).count();
    let row = rest[..digits].parse::<usize>().ok()?;
    Some((row, prefix, rest[digits..].to_string()))
}

/// Rewrite a tuple field-run error: the flat index equals the outer row, so
/// only the element label (index, or name for a named tuple) is inserted.
fn remap_tuple_element_err(py: Python<'_>, name: &str, label: &str, err: PyErr) -> PyErr {
    let Some((row, prefix, tail)) = parse_row_err(py, name, &err) else {
        return err;
    };
    PyValueError::new_err(format!("{prefix}{row} element {label}{tail}"))
}

/// Rewrite an alternative's dense child row index to its logical Variant row.
pub(super) fn remap_variant_child_err(
    py: Python<'_>,
    name: &str,
    discriminators: &[u8],
    alternative: usize,
    err: PyErr,
) -> PyErr {
    let Some((dense_row, prefix, tail)) = parse_row_err(py, name, &err) else {
        return err;
    };
    let Ok(discriminator) = u8::try_from(alternative) else {
        return err;
    };
    let Some(logical_row) = discriminators
        .iter()
        .enumerate()
        .filter_map(|(row, &disc)| (disc == discriminator).then_some(row))
        .nth(dense_row)
    else {
        return err;
    };
    PyValueError::new_err(format!("{prefix}{logical_row}{tail}"))
}

/// Append one Array row's elements to the flat run. Exact list/tuple rows
/// copy borrowed pointers without running Python; anything else keeps the
/// generic path's accepted containers and error messages via `list.extend`.
fn flatten_array_row(
    py: Python<'_>,
    name: &str,
    inner: &ChType,
    value: &Bound<'_, PyAny>,
    row: usize,
    flat: &mut FlatRefs,
) -> PyResult<()> {
    if let Ok(list) = value.downcast_exact::<PyList>() {
        // SAFETY: copying exact-list items runs no Python code.
        unsafe { flat.extend_from_seq(&ListSeq(list)) };
        return Ok(());
    }
    if let Ok(tuple) = value.downcast_exact::<PyTuple>() {
        // SAFETY: copying exact-tuple items runs no Python code.
        unsafe { flat.extend_from_seq(&TupleSeq(tuple)) };
        return Ok(());
    }
    if value.is_none() {
        return Err(PyValueError::new_err(format!(
            "column {name:?} row {row} is None but Array({inner}) is not Nullable"
        )));
    }
    if value.downcast::<PyString>().is_ok() {
        return Err(PyValueError::new_err(format!(
            "column {name:?} row {row} is a str, not an Array sequence"
        )));
    }
    // bytes-like rows flatten as int elements, matching the python codec's
    // `data.extend(row)` iteration semantics.
    if let Ok(bytes) = value.downcast::<PyBytes>() {
        return flat.extend_from_byte_run(py, bytes.as_bytes());
    }
    if let Ok(bytes) = value.downcast::<PyByteArray>() {
        return flat.extend_from_byte_run(py, &bytes.to_vec());
    }
    if value.downcast::<PySet>().is_ok()
        || value.downcast::<PyFrozenSet>().is_ok()
        || value.downcast::<PyDict>().is_ok()
    {
        return Err(PyValueError::new_err(format!(
            "column {name:?} row {row} is an unordered set/dict, which has no defined Array element order"
        )));
    }
    let items = PyList::empty(py);
    items
        .call_method1(intern!(py, "extend"), (value,))
        .map_err(|err| {
            let wrapped = PyValueError::new_err(format!(
                "column {name:?} row {row} is not a valid Array value"
            ));
            wrapped.set_cause(py, Some(err));
            wrapped
        })?;
    // SAFETY: items is an owned exact list; copying its items runs no Python
    // code and the strong references outlive the temporary list.
    unsafe { flat.extend_from_seq(&ListSeq(&items)) };
    Ok(())
}

/// Strong references to flattened Array elements. Holding a reference per
/// element keeps every pointer in the run valid across any Python code the
/// element conversion runs, so the run can be consumed with borrowed reads.
#[derive(Default)]
pub(super) struct FlatRefs {
    pub(super) ptrs: Vec<*mut ffi::PyObject>,
}

impl FlatRefs {
    /// Append every element of `seq` as a strong reference.
    ///
    /// # Safety
    ///
    /// Requires the GIL; `seq` must allow borrowed reads with no Python code
    /// running during the copy (an exact list or tuple).
    unsafe fn extend_from_seq<S: FastSeq>(&mut self, seq: &S) {
        let len = seq.size();
        self.ptrs.reserve(len);
        for index in 0..len {
            let item = seq.get(index);
            ffi::Py_INCREF(item);
            self.ptrs.push(item);
        }
    }

    /// Append each byte of `bytes` as an owned Python int element.
    fn extend_from_byte_run(&mut self, py: Python<'_>, bytes: &[u8]) -> PyResult<()> {
        self.ptrs.reserve(bytes.len());
        for &byte in bytes {
            // SAFETY: GIL held; PyLong_FromLong returns an owned reference
            // (a cached small int here) that Drop releases.
            let item = unsafe { ffi::PyLong_FromLong(c_long::from(byte)) };
            if item.is_null() {
                return Err(PyErr::fetch(py));
            }
            self.ptrs.push(item);
        }
        Ok(())
    }

    /// Append a strong reference to `obj`.
    pub(super) fn push_ref(&mut self, obj: &Bound<'_, PyAny>) {
        // SAFETY: GIL held (the Bound proves it); Drop releases the reference.
        unsafe { ffi::Py_INCREF(obj.as_ptr()) };
        self.ptrs.push(obj.as_ptr());
    }

    /// Append a strong reference to None.
    fn push_none(&mut self, _py: Python<'_>) {
        // SAFETY: GIL held; Py_None is a valid object pointer.
        unsafe {
            let none = ffi::Py_None();
            ffi::Py_INCREF(none);
            self.ptrs.push(none);
        }
    }

    /// Current run length as an i64 offset. `unit` names what the run counts
    /// ("Array element", "Map entry").
    fn end_offset(&self, name: &str, unit: &str) -> PyResult<i64> {
        i64::try_from(self.ptrs.len()).map_err(|_| {
            PyValueError::new_err(format!(
                "column {name:?} {unit} count exceeds i64 offset capacity"
            ))
        })
    }
}

impl Drop for FlatRefs {
    fn drop(&mut self) {
        // SAFETY: FlatRefs is only built and dropped inside a frame that holds
        // a `Python` token, so the GIL is held here.
        for &ptr in &self.ptrs {
            unsafe { ffi::Py_DECREF(ptr) };
        }
    }
}

/// Build the flattened Array element column. Mirrors `build_column`'s wrapper
/// dispatch with the flat run standing in for the Python container, feeding
/// the same seq fast paths and generic scalar loops.
pub(super) fn build_element_column(
    py: Python<'_>,
    name: &str,
    ch_type: &ChType,
    ptrs: &[*mut ffi::PyObject],
) -> PyResult<Column> {
    let row_count = ptrs.len();
    let seq = PtrSeq(ptrs);
    // Expand SimpleAggregateFunction/geo/Nested aliases before dispatch so a
    // nested alias (e.g. Array(Point)) builds its physical element column.
    if let Some(delegate) = ch_type.physical_delegate() {
        return build_element_column(py, name, &delegate, ptrs);
    }
    match ch_type {
        ChType::Nothing => Ok(nothing_column_from_seq(&seq, row_count, false)),
        ChType::AggregateFunction { .. } => {
            aggregate_state_column_from_seq(py, name, ch_type, &seq, row_count)
        }
        ChType::Array(inner) => array_column_from_seq(py, name, inner, &seq, row_count),
        ChType::QBit {
            element_type,
            dimension,
        } => {
            build_qbit_column_from_seq(py, name, *element_type, *dimension, &seq, row_count, false)
        }
        ChType::Tuple(elements) => {
            tuple_column_from_seq(py, name, elements, &seq, row_count, false)
        }
        ChType::Map(key, value) => map_column_from_seq(py, name, key, value, &seq, row_count),
        ChType::Variant(alternatives) => {
            variant_column_from_seq(py, name, alternatives, &seq, row_count)
        }
        ChType::Dynamic { .. } => dynamic_string_column(&PtrRows { py, ptrs }, row_count),
        ChType::Json { .. } => {
            json_text_column_from_rows(py, name, &PtrRows { py, ptrs }, row_count, false)
        }
        ChType::Nullable(inner) => {
            // Nullable(Point) -> Nullable(Tuple), Nullable(SAF(T)) -> Nullable(T);
            // the physical delegate governs the nullable element shape.
            if let Some(delegate) = inner.physical_delegate() {
                return build_element_column(py, name, &ChType::Nullable(Box::new(delegate)), ptrs);
            }
            if let ChType::Tuple(elements) = inner.as_ref() {
                return tuple_column_from_seq(py, name, elements, &seq, row_count, true);
            }
            if matches!(inner.as_ref(), ChType::Nothing) {
                return Ok(nothing_column_from_seq(&seq, row_count, true));
            }
            if matches!(inner.as_ref(), ChType::Json { .. }) {
                return json_text_column_from_rows(
                    py,
                    name,
                    &PtrRows { py, ptrs },
                    row_count,
                    true,
                );
            }
            if let ChType::QBit {
                element_type,
                dimension,
            } = inner.as_ref()
            {
                return build_qbit_column_from_seq(
                    py,
                    name,
                    *element_type,
                    *dimension,
                    &seq,
                    row_count,
                    true,
                );
            }
            if matches!(
                inner.as_ref(),
                ChType::Nullable(_)
                    | ChType::LowCardinality(_)
                    | ChType::Array(_)
                    | ChType::Map(..)
            ) {
                return Err(PyNotImplementedError::new_err(format!(
                    "unsupported Nullable inner type {inner} for column {name:?}"
                )));
            }
            if let Some(column) = try_fast_column_seq(py, name, inner, &seq, row_count, true)? {
                return Ok(column);
            }
            nullable_scalar_column(py, name, inner, &PtrRows { py, ptrs }, row_count)
        }
        ChType::LowCardinality(inner) => {
            // Resolve the physical dict value type: strip the SAF chain, unwrap an
            // optional Nullable, strip again. So LowCardinality(SAF(anyLast, String))
            // and LowCardinality(SAF(anyLast, Nullable(String))) reach the String path.
            let (nullable, value_type) = low_cardinality_dict_value_type(inner);
            if !is_low_cardinality_inner(value_type) {
                return Err(PyNotImplementedError::new_err(format!(
                    "unsupported LowCardinality inner type {value_type} for column {name:?}"
                )));
            }
            if matches!(value_type, ChType::String) {
                return lc_string_seq(py, name, value_type, &seq, row_count, nullable);
            }
            if wide_int_layout(value_type).is_some() {
                return lc_wide_column(
                    py,
                    name,
                    value_type,
                    &PtrRows { py, ptrs },
                    row_count,
                    nullable,
                );
            }
            lc_scalar_column(
                py,
                name,
                value_type,
                &PtrRows { py, ptrs },
                row_count,
                nullable,
            )
        }
        _ => {
            if let Some(column) = try_fast_column_seq(py, name, ch_type, &seq, row_count, false)? {
                return Ok(column);
            }
            plain_scalar_column(py, name, ch_type, &PtrRows { py, ptrs }, row_count)
        }
    }
}

/// Build a `Tuple(T1, ...)` column: each row fans out into one strong-ref run
/// per element, then each element column is built once over its run, hitting
/// the same per-type fast paths as a plain column. `nullable` builds the
/// tuple-level validity of a `Nullable(Tuple)`; a None row keeps the children
/// full length with per-type default placeholders.
pub(super) fn build_tuple_column(
    py: Python<'_>,
    name: &str,
    elements: &[(Option<String>, ChType)],
    values: &Bound<'_, PyAny>,
    row_count: usize,
    nullable: bool,
) -> PyResult<Column> {
    let column_values = ColumnValues::new(values, name)?;
    check_row_count(name, &column_values, row_count)?;
    if let Ok(list) = values.downcast_exact::<PyList>() {
        return tuple_column_from_seq(py, name, elements, &ListSeq(list), row_count, nullable);
    }
    if let Ok(tuple) = values.downcast_exact::<PyTuple>() {
        return tuple_column_from_seq(py, name, elements, &TupleSeq(tuple), row_count, nullable);
    }
    let mut builder = TupleBuilder::new(py, name, elements, row_count, nullable);
    for row in 0..row_count {
        let value = column_values.get_item(row)?;
        builder.push_row(&value, row)?;
    }
    builder.finish()
}

/// Tuple rows over an exact list or tuple of rows, or a flattened element run.
fn tuple_column_from_seq<S: FastSeq>(
    py: Python<'_>,
    name: &str,
    elements: &[(Option<String>, ChType)],
    seq: &S,
    row_count: usize,
    nullable: bool,
) -> PyResult<Column> {
    let mut builder = TupleBuilder::new(py, name, elements, row_count, nullable);
    for row in 0..row_count {
        // SAFETY: row < row_count, the container size the caller checked and
        // every fallback revalidates; the strong reference keeps the row
        // alive across any Python code the row read runs.
        let value = unsafe { Bound::from_borrowed_ptr(py, seq.get(row)) };
        builder.push_row(&value, row)?;
        check_not_resized(seq, name, row_count)?;
    }
    builder.finish()
}

/// Streaming Tuple row builder: one strong-ref run per element, an optional
/// tuple-level null map, and a row-read mode decided by the first non-None
/// row (element-name dict reads for a fully named tuple whose first non-None
/// row is a dict, positional reads otherwise).
struct TupleBuilder<'a, 'py> {
    py: Python<'py>,
    name: &'a str,
    elements: &'a [(Option<String>, ChType)],
    flats: Vec<FlatRefs>,
    null_map: Option<Vec<u8>>,
    defaults: Option<Vec<Py<PyAny>>>,
    names: Option<Vec<Bound<'py, PyString>>>,
    dict_mode: Option<bool>,
    row_count: usize,
}

impl<'a, 'py> TupleBuilder<'a, 'py> {
    fn new(
        py: Python<'py>,
        name: &'a str,
        elements: &'a [(Option<String>, ChType)],
        row_count: usize,
        nullable: bool,
    ) -> Self {
        let names: Option<Vec<Bound<'py, PyString>>> =
            (!elements.is_empty() && elements.iter().all(|(n, _)| n.is_some())).then(|| {
                elements
                    .iter()
                    .map(|(n, _)| PyString::new(py, n.as_deref().unwrap_or_default()))
                    .collect()
            });
        Self {
            py,
            name,
            elements,
            flats: elements
                .iter()
                .map(|_| FlatRefs {
                    ptrs: Vec::with_capacity(row_count),
                })
                .collect(),
            null_map: nullable.then(|| Vec::with_capacity(row_count)),
            defaults: None,
            names,
            dict_mode: None,
            row_count,
        }
    }

    fn tuple_type(&self) -> ChType {
        ChType::Tuple(self.elements.to_vec())
    }

    fn push_row(&mut self, value: &Bound<'_, PyAny>, row: usize) -> PyResult<()> {
        if value.is_none() {
            if self.null_map.is_none() {
                return Err(PyValueError::new_err(format!(
                    "column {:?} row {row} is None but {} is not Nullable",
                    self.name,
                    self.tuple_type()
                )));
            }
            if self.defaults.is_none() {
                self.defaults = Some(
                    self.elements
                        .iter()
                        .map(|(_, element_type)| default_pyobject(self.py, element_type))
                        .collect::<PyResult<_>>()?,
                );
            }
            if let Some(null_map) = &mut self.null_map {
                null_map.push(1);
            }
            if let Some(defaults) = &self.defaults {
                for (flat, default) in self.flats.iter_mut().zip(defaults) {
                    flat.push_ref(default.bind(self.py));
                }
            }
            return Ok(());
        }
        if let Some(null_map) = &mut self.null_map {
            null_map.push(0);
        }
        let dict_mode = match self.dict_mode {
            Some(mode) => mode,
            None => {
                let mode = self.names.is_some() && value.is_instance_of::<PyDict>();
                self.dict_mode = Some(mode);
                mode
            }
        };
        if dict_mode {
            self.push_dict_row(value, row)
        } else {
            self.push_positional_row(value, row)
        }
    }

    /// Read one row by element name: missing keys become None, extra keys are
    /// ignored. Non-dict rows fall back to a `.get` method read.
    fn push_dict_row(&mut self, value: &Bound<'_, PyAny>, row: usize) -> PyResult<()> {
        let Some(names) = &self.names else {
            return Err(PyValueError::new_err("internal tuple row mode mismatch"));
        };
        if let Ok(dict) = value.downcast_exact::<PyDict>() {
            for (flat, key) in self.flats.iter_mut().zip(names) {
                match dict.get_item(key)? {
                    Some(item) => flat.push_ref(&item),
                    None => flat.push_none(self.py),
                }
            }
            return Ok(());
        }
        let get = value.getattr(intern!(self.py, "get")).map_err(|err| {
            let wrapped = PyValueError::new_err(format!(
                "column {:?} row {row} cannot be read as a dict for the named Tuple",
                self.name
            ));
            wrapped.set_cause(self.py, Some(err));
            wrapped
        })?;
        for (flat, key) in self.flats.iter_mut().zip(names) {
            let item = get.call1((key,)).map_err(|err| {
                let wrapped = PyValueError::new_err(format!(
                    "column {:?} row {row} element {key:?} cannot be read from the dict-like row",
                    self.name
                ));
                wrapped.set_cause(self.py, Some(err));
                wrapped
            })?;
            flat.push_ref(&item);
        }
        Ok(())
    }

    /// Read one row positionally. Exact list/tuple rows copy borrowed
    /// pointers; other iterables flatten through `list.extend`.
    fn push_positional_row(&mut self, value: &Bound<'_, PyAny>, row: usize) -> PyResult<()> {
        if let Ok(list) = value.downcast_exact::<PyList>() {
            // SAFETY: copying exact-list items runs no Python code.
            return unsafe { self.push_positional_seq(&ListSeq(list), row) };
        }
        if let Ok(tuple) = value.downcast_exact::<PyTuple>() {
            // SAFETY: copying exact-tuple items runs no Python code.
            return unsafe { self.push_positional_seq(&TupleSeq(tuple), row) };
        }
        if value.downcast::<PyString>().is_ok() {
            return Err(PyValueError::new_err(format!(
                "column {:?} row {row} is a str, not a Tuple row",
                self.name
            )));
        }
        if value.is_instance_of::<PyDict>() {
            return Err(PyValueError::new_err(format!(
                "column {:?} row {row} is a dict but Tuple rows are read positionally",
                self.name
            )));
        }
        if value.downcast::<PySet>().is_ok() || value.downcast::<PyFrozenSet>().is_ok() {
            return Err(PyValueError::new_err(format!(
                "column {:?} row {row} is an unordered set, which has no defined Tuple element order",
                self.name
            )));
        }
        let items = PyList::empty(self.py);
        items
            .call_method1(intern!(self.py, "extend"), (value,))
            .map_err(|err| {
                let wrapped = PyValueError::new_err(format!(
                    "column {:?} row {row} is not a valid Tuple row",
                    self.name
                ));
                wrapped.set_cause(self.py, Some(err));
                wrapped
            })?;
        // SAFETY: items is an owned exact list; copying runs no Python code.
        unsafe { self.push_positional_seq(&ListSeq(&items), row) }
    }

    /// Copy one arity-checked row into the element runs.
    ///
    /// # Safety
    ///
    /// Requires the GIL; `seq` must allow borrowed reads with no Python code
    /// running during the copy (an exact list or tuple).
    unsafe fn push_positional_seq<S: FastSeq>(&mut self, seq: &S, row: usize) -> PyResult<()> {
        let got = seq.size();
        if got != self.elements.len() {
            return Err(PyValueError::new_err(format!(
                "column {:?} row {row} has {got} elements but the Tuple declares {}",
                self.name,
                self.elements.len()
            )));
        }
        for (index, flat) in self.flats.iter_mut().enumerate() {
            let item = seq.get(index);
            ffi::Py_INCREF(item);
            flat.ptrs.push(item);
        }
        Ok(())
    }

    fn finish(self) -> PyResult<Column> {
        let mut fields = Vec::with_capacity(self.elements.len());
        for (index, ((element_name, element_type), flat)) in
            self.elements.iter().zip(&self.flats).enumerate()
        {
            let label = match element_name {
                Some(n) => format!("{n:?}"),
                None => index.to_string(),
            };
            let column = build_element_column(self.py, self.name, element_type, &flat.ptrs)
                .map_err(|err| remap_tuple_element_err(self.py, self.name, &label, err))?;
            fields.push(column);
        }
        Ok(Column::Tuple(match self.null_map {
            Some(nulls) => {
                TupleColumn::new_nullable(fields, self.row_count, Bitmap::from_ch_null_map(&nulls))
            }
            None => TupleColumn::new(fields, self.row_count),
        }))
    }
}

/// Build a `Map(K, V)` column: dict rows flatten into a key run and a value
/// run with an Arrow list offsets run; the two entry columns are built once
/// over their flat runs. Maps are never nullable at the map level, so a None
/// row is an error.
pub(super) fn build_map_column(
    py: Python<'_>,
    name: &str,
    key_type: &ChType,
    value_type: &ChType,
    values: &Bound<'_, PyAny>,
    row_count: usize,
) -> PyResult<Column> {
    let column_values = ColumnValues::new(values, name)?;
    check_row_count(name, &column_values, row_count)?;
    if let Ok(list) = values.downcast_exact::<PyList>() {
        return map_column_from_seq(py, name, key_type, value_type, &ListSeq(list), row_count);
    }
    if let Ok(tuple) = values.downcast_exact::<PyTuple>() {
        return map_column_from_seq(py, name, key_type, value_type, &TupleSeq(tuple), row_count);
    }
    let mut builder = MapBuilder::new(py, name, key_type, value_type, row_count);
    for row in 0..row_count {
        let value = column_values.get_item(row)?;
        builder.push_row(&value, row)?;
    }
    builder.finish()
}

/// Map rows over an exact list or tuple of rows, or a flattened element run.
fn map_column_from_seq<S: FastSeq>(
    py: Python<'_>,
    name: &str,
    key_type: &ChType,
    value_type: &ChType,
    seq: &S,
    row_count: usize,
) -> PyResult<Column> {
    let mut builder = MapBuilder::new(py, name, key_type, value_type, row_count);
    for row in 0..row_count {
        // SAFETY: row < row_count, the container size the caller checked and
        // every fallback revalidates; the strong reference keeps the row
        // alive across any Python code the row read runs.
        let value = unsafe { Bound::from_borrowed_ptr(py, seq.get(row)) };
        builder.push_row(&value, row)?;
        check_not_resized(seq, name, row_count)?;
    }
    builder.finish()
}

/// Streaming Map row builder: an offsets run plus parallel key and value
/// strong-ref runs. Exact dict rows copy entries without running Python;
/// dict-like rows read through `.items()`.
struct MapBuilder<'a, 'py> {
    py: Python<'py>,
    name: &'a str,
    key_type: &'a ChType,
    value_type: &'a ChType,
    offsets: Vec<i64>,
    keys: FlatRefs,
    values: FlatRefs,
}

impl<'a, 'py> MapBuilder<'a, 'py> {
    fn new(
        py: Python<'py>,
        name: &'a str,
        key_type: &'a ChType,
        value_type: &'a ChType,
        row_count: usize,
    ) -> Self {
        let mut offsets = Vec::with_capacity(row_count + 1);
        offsets.push(0i64);
        Self {
            py,
            name,
            key_type,
            value_type,
            offsets,
            keys: FlatRefs::default(),
            values: FlatRefs::default(),
        }
    }

    fn push_row(&mut self, value: &Bound<'_, PyAny>, row: usize) -> PyResult<()> {
        if value.is_none() {
            return Err(PyValueError::new_err(format!(
                "column {:?} row {row} is None but Map({}, {}) is not Nullable",
                self.name, self.key_type, self.value_type
            )));
        }
        if let Ok(dict) = value.downcast_exact::<PyDict>() {
            for (key, val) in dict.iter() {
                self.keys.push_ref(&key);
                self.values.push_ref(&val);
            }
        } else {
            self.push_mapping_row(value, row)?;
        }
        self.offsets
            .push(self.keys.end_offset(self.name, "Map entry")?);
        Ok(())
    }

    /// Dict-like fallback: rows must expose `.items()`; anything else is not
    /// a Map row. Pair iterables are deliberately not accepted.
    fn push_mapping_row(&mut self, value: &Bound<'_, PyAny>, row: usize) -> PyResult<()> {
        let not_a_dict = |err: PyErr| {
            let wrapped = PyValueError::new_err(format!(
                "column {:?} row {row} is not a dict for Map({}, {})",
                self.name, self.key_type, self.value_type
            ));
            wrapped.set_cause(self.py, Some(err));
            wrapped
        };
        let items = value
            .call_method0(intern!(self.py, "items"))
            .map_err(not_a_dict)?;
        let entries = PyList::empty(self.py);
        entries
            .call_method1(intern!(self.py, "extend"), (items,))
            .map_err(not_a_dict)?;
        for index in 0..entries.len() {
            let entry = entries.get_item(index)?;
            let (key, val) = entry
                .extract::<(Bound<'_, PyAny>, Bound<'_, PyAny>)>()
                .map_err(|err| {
                    let wrapped = PyValueError::new_err(format!(
                        "column {:?} row {row} Map entry is not a key/value pair",
                        self.name
                    ));
                    wrapped.set_cause(self.py, Some(err));
                    wrapped
                })?;
            self.keys.push_ref(&key);
            self.values.push_ref(&val);
        }
        Ok(())
    }

    fn finish(self) -> PyResult<Column> {
        let total = self.keys.ptrs.len();
        let keys_column = build_element_column(self.py, self.name, self.key_type, &self.keys.ptrs)
            .map_err(|err| remap_flat_err(self.py, self.name, &self.offsets, "key", err))?;
        let values_column =
            build_element_column(self.py, self.name, self.value_type, &self.values.ptrs)
                .map_err(|err| remap_flat_err(self.py, self.name, &self.offsets, "value", err))?;
        let entries = Column::Tuple(TupleColumn::new(vec![keys_column, values_column], total));
        Ok(Column::Map(MapColumn::new(self.offsets, entries)))
    }
}
