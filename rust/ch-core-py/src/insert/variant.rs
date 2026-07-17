use super::*;

/// Build a Variant column in one row scan: one discriminator byte per logical
/// row and one strong-reference run per dense alternative. Each alternative is
/// then built once through the existing column fast paths.
pub(super) fn build_variant_column(
    py: Python<'_>,
    name: &str,
    alternatives: &[ChType],
    values: &Bound<'_, PyAny>,
    row_count: usize,
) -> PyResult<Column> {
    let column_values = ColumnValues::new(values, name)?;
    check_row_count(name, &column_values, row_count)?;
    if let Ok(list) = values.downcast_exact::<PyList>() {
        return variant_column_from_seq(py, name, alternatives, &ListSeq(list), row_count);
    }
    if let Ok(tuple) = values.downcast_exact::<PyTuple>() {
        return variant_column_from_seq(py, name, alternatives, &TupleSeq(tuple), row_count);
    }

    let mut builder = VariantBuilder::new(py, name, alternatives, row_count)?;
    for row in 0..row_count {
        let value = column_values.get_item(row)?;
        builder.push_row(&value, row)?;
    }
    builder.finish()
}

/// Variant rows over an exact list/tuple or a flattened container element run.
pub(super) fn variant_column_from_seq<S: FastSeq>(
    py: Python<'_>,
    name: &str,
    alternatives: &[ChType],
    seq: &S,
    row_count: usize,
) -> PyResult<Column> {
    let mut builder = VariantBuilder::new(py, name, alternatives, row_count)?;
    for row in 0..row_count {
        // SAFETY: row < row_count, which the caller checked against seq.size().
        // A strong reference protects the value while the builder retains its
        // selected payload in an alternative run.
        let value = unsafe { Bound::from_borrowed_ptr(py, seq.get(row)) };
        let ran_python = builder.push_row(&value, row)?;
        if ran_python {
            // Dispatch that ran Python (subclass attribute access or a
            // str-subclass name lookup) can resize the source. Drop the
            // temporary strong reference first so a finalizer cannot resize
            // it after this validation.
            drop(value);
            check_not_resized(seq, name, row_count)?;
        }
    }
    builder.finish()
}

/// The driver Variant instance's dispatch tables, resolved once per column.
struct VariantDispatch<'py> {
    /// Snapshot of `_python_map`: one strong type-object reference and its
    /// discriminator per unambiguous alternative.
    types: Vec<(Py<PyAny>, u8)>,
    name_index: Bound<'py, PyDict>,
    typed_variant_type: Bound<'py, PyAny>,
}

/// Per-column Variant dispatch state. The Python driver's Variant instance is
/// cached by canonical type name; snapshotting its `_python_map` and borrowing
/// its `_name_index` keeps this binding's exact-type inference and
/// `typed_variant` policy identical to the established Python encoder,
/// including collision removal.
struct VariantBuilder<'a, 'py> {
    py: Python<'py>,
    name: &'a str,
    alternatives: &'a [ChType],
    type_name: String,
    dispatch: VariantDispatch<'py>,
    discriminators: Vec<u8>,
    values: Vec<FlatRefs>,
    /// Explicit tag names already resolved through `_name_index`, with their
    /// discriminators. Rows reuse the same exact-str tag objects, so a pointer
    /// compare replaces the dict lookup. The strong references keep each
    /// cached name alive, so a cached pointer can never be a freed-and-reused
    /// address; exact-str-only entries mean the hit path runs no Python code.
    tag_cache: Vec<(Py<PyAny>, usize)>,
}

impl<'a, 'py> VariantBuilder<'a, 'py> {
    fn new(
        py: Python<'py>,
        name: &'a str,
        alternatives: &'a [ChType],
        row_count: usize,
    ) -> PyResult<Self> {
        let type_name = ChType::Variant(alternatives.to_vec()).to_string();
        // Any driver-interop failure becomes NotImplementedError so the
        // insert probe falls back to the Python codec.
        let dispatch =
            Self::driver_dispatch(py, &type_name, alternatives.len()).map_err(|err: PyErr| {
                PyNotImplementedError::new_err(format!(
                    "Variant type {type_name} is not supported by the native encoder: {err}"
                ))
            })?;
        Ok(Self {
            py,
            name,
            alternatives,
            type_name,
            dispatch,
            discriminators: Vec::with_capacity(row_count),
            values: alternatives.iter().map(|_| FlatRefs::default()).collect(),
            tag_cache: Vec::new(),
        })
    }

    /// Resolve the driver's cached Variant instance and snapshot its dispatch
    /// tables.
    fn driver_dispatch(
        py: Python<'py>,
        type_name: &str,
        alternative_count: usize,
    ) -> PyResult<VariantDispatch<'py>> {
        let py_variant = py
            .import("clickhouse_connect.datatypes.registry")?
            .getattr("get_from_name")?
            .call1((type_name,))?;
        let python_map = py_variant
            .getattr("_python_map")?
            .downcast_into::<PyDict>()?;
        let name_index = py_variant
            .getattr("_name_index")?
            .downcast_into::<PyDict>()?;
        let typed_variant_type = py
            .import("clickhouse_connect.datatypes.dynamic")?
            .getattr("TypedVariant")?;
        let mut types = Vec::with_capacity(python_map.len());
        for (key, value) in python_map.iter() {
            // SAFETY: value is a live dict value kept alive by its Bound.
            // PyLong_AsSize_t raises TypeError on a non-int without running
            // Python code.
            let index = unsafe { ffi::PyLong_AsSize_t(value.as_ptr()) };
            if index == usize::MAX && unsafe { !ffi::PyErr_Occurred().is_null() } {
                return Err(PyErr::fetch(py));
            }
            let discriminator = u8::try_from(index)
                .ok()
                .filter(|&d| usize::from(d) < alternative_count)
                .ok_or_else(|| PyValueError::new_err("Variant dispatch index is out of range"))?;
            types.push((key.unbind(), discriminator));
        }
        Ok(VariantDispatch {
            types,
            name_index,
            typed_variant_type,
        })
    }

    /// Select and retain one row. The boolean reports whether successful
    /// dispatch ran Python code, so borrowed-sequence callers only pay for a
    /// resize check when it did.
    fn push_row(&mut self, value: &Bound<'py, PyAny>, row: usize) -> PyResult<bool> {
        if value.is_none() {
            self.discriminators.push(u8::MAX);
            return Ok(false);
        }

        // SAFETY: value is a live Python object. Reading its type pointer and
        // comparing it against snapshot pointers cannot invoke user Python
        // code.
        let value_type = unsafe { ffi::Py_TYPE(value.as_ptr()) }.cast::<ffi::PyObject>();
        if value_type == self.dispatch.typed_variant_type.as_ptr()
            && unsafe { ffi::PyTuple_GET_SIZE(value.as_ptr()) } == 2
        {
            // Exact two-slot namedtuple layout: slots 0/1 are direct reads.
            // An exact instance with any other size falls through to the safe
            // attribute path below.
            let payload = unsafe {
                Bound::from_borrowed_ptr(self.py, ffi::PyTuple_GET_ITEM(value.as_ptr(), 0))
            };
            let explicit_name = unsafe {
                Bound::from_borrowed_ptr(self.py, ffi::PyTuple_GET_ITEM(value.as_ptr(), 1))
            };
            return self.push_explicit(&payload, &explicit_name, row);
        }

        if let Some(discriminator) =
            self.dispatch.types.iter().find_map(|(key, discriminator)| {
                (value_type == key.as_ptr()).then_some(*discriminator)
            })
        {
            self.push_selected(usize::from(discriminator), value)?;
            return Ok(false);
        }

        // A dispatch miss checks for a TypedVariant subtype before failing.
        let is_typed_variant = unsafe {
            ffi::PyObject_TypeCheck(
                value.as_ptr(),
                self.dispatch
                    .typed_variant_type
                    .as_ptr()
                    .cast::<ffi::PyTypeObject>(),
            ) != 0
        };
        if is_typed_variant {
            // A subclass (or a mis-sized exact instance) is not guaranteed to
            // have the base tuple's physical two-slot layout. Match Python's
            // isinstance policy through safe attribute reads; the outer
            // sequence path rechecks resize after this call because these
            // lookups may execute Python.
            let payload = value.getattr("value")?;
            let explicit_name = value.getattr("type_name")?;
            self.push_explicit(&payload, &explicit_name, row)?;
            return Ok(true);
        }

        Err(PyValueError::new_err(format!(
            "column {:?} row {row} cannot map Python type {} to any member of {}",
            self.name,
            python_type_name(value.as_ptr()),
            self.type_name
        )))
    }

    /// Push an explicitly-tagged row. The boolean reports whether the name
    /// lookup may have run Python code (a str-subclass name's `__hash__` and
    /// `__eq__` run during the dict lookup).
    fn push_explicit(
        &mut self,
        payload: &Bound<'py, PyAny>,
        explicit_name: &Bound<'py, PyAny>,
        row: usize,
    ) -> PyResult<bool> {
        if unsafe { ffi::PyUnicode_Check(explicit_name.as_ptr()) } == 0 {
            return Err(PyValueError::new_err(format!(
                "column {:?} row {row} typed Variant name must be a str",
                self.name
            )));
        }
        let ran_python = unsafe { ffi::PyUnicode_CheckExact(explicit_name.as_ptr()) } == 0;
        if !ran_python {
            if let Some(&(_, discriminator)) = self
                .tag_cache
                .iter()
                .find(|(cached, _)| cached.as_ptr() == explicit_name.as_ptr())
            {
                self.push_selected(discriminator, payload)?;
                return Ok(false);
            }
        }
        let discriminator =
            unsafe { dict_index(&self.dispatch.name_index, explicit_name.as_ptr())? }.ok_or_else(
                || {
                    let rendered = explicit_name
                        .extract::<String>()
                        .unwrap_or_else(|_| "<invalid>".to_string());
                    PyValueError::new_err(format!(
                        "column {:?} row {row} type {rendered:?} is not a member of {}",
                        self.name, self.type_name
                    ))
                },
            )?;
        // Bound so a column with many distinct exact-str tag objects cannot
        // grow an unbounded linear scan; misses fall back to the dict lookup.
        if !ran_python && self.tag_cache.len() < 16 {
            self.tag_cache
                .push((explicit_name.clone().unbind(), discriminator));
        }
        self.push_selected(discriminator, payload)?;
        Ok(ran_python)
    }

    fn push_selected(&mut self, discriminator: usize, payload: &Bound<'py, PyAny>) -> PyResult<()> {
        let discriminator = u8::try_from(discriminator).map_err(|_| {
            PyValueError::new_err("internal error: Variant discriminator exceeds UInt8 range")
        })?;
        if usize::from(discriminator) >= self.values.len() {
            return Err(PyValueError::new_err(
                "internal error: Variant dispatch index is out of range",
            ));
        }
        self.discriminators.push(discriminator);
        self.values[usize::from(discriminator)].push_ref(payload);
        Ok(())
    }

    fn finish(self) -> PyResult<Column> {
        let mut children = Vec::with_capacity(self.alternatives.len());
        for (alternative_index, (alternative, values)) in
            self.alternatives.iter().zip(&self.values).enumerate()
        {
            children.push(
                build_element_column(self.py, self.name, alternative, &values.ptrs).map_err(
                    |err| {
                        remap_variant_child_err(
                            self.py,
                            self.name,
                            &self.discriminators,
                            alternative_index,
                            err,
                        )
                    },
                )?,
            );
        }
        let variant = VariantColumn::try_new(&self.discriminators, children).map_err(|err| {
            PyValueError::new_err(format!(
                "column {:?} has an invalid Variant layout: {err}",
                self.name
            ))
        })?;
        Ok(Column::Variant(variant))
    }
}

/// Borrow a non-negative Python integer index from an exact dict lookup.
///
/// # Safety
///
/// `key` must be a live Python object and the GIL must be held.
unsafe fn dict_index(dict: &Bound<'_, PyDict>, key: *mut ffi::PyObject) -> PyResult<Option<usize>> {
    let value = ffi::PyDict_GetItemWithError(dict.as_ptr(), key);
    if value.is_null() {
        if ffi::PyErr_Occurred().is_null() {
            return Ok(None);
        }
        return Err(PyErr::fetch(dict.py()));
    }
    let index = ffi::PyLong_AsSize_t(value);
    if index == usize::MAX && !ffi::PyErr_Occurred().is_null() {
        return Err(PyErr::fetch(dict.py()));
    }
    Ok(Some(index))
}

/// Exact CPython type name without allocating or invoking Python code.
pub(super) fn python_type_name(value: *mut ffi::PyObject) -> String {
    // SAFETY: value is live and tp_name is a NUL-terminated string owned by its
    // type for at least the lifetime of the object.
    unsafe { std::ffi::CStr::from_ptr((*ffi::Py_TYPE(value)).tp_name) }
        .to_string_lossy()
        .into_owned()
}

/// Default placeholder Python object for a null `Nullable(Tuple)` row's
/// element, matching the wire defaults the server writes for null rows.
pub(super) fn default_pyobject(py: Python<'_>, ch_type: &ChType) -> PyResult<Py<PyAny>> {
    if let Some(delegate) = ch_type.physical_delegate() {
        return default_pyobject(py, &delegate);
    }
    Ok(match ch_type {
        // A Dynamic placeholder stays None; the String insert path renders it
        // as the literal "NULL".
        ChType::Nullable(_) | ChType::Variant(_) | ChType::Dynamic { .. } => py.None(),
        ChType::LowCardinality(inner) => default_pyobject(py, inner)?,
        ChType::Nothing => py.None(),
        ChType::Bool => PyBool::new(py, false).to_owned().into_any().unbind(),
        ChType::String | ChType::FixedString(_) => PyString::new(py, "").into_any().unbind(),
        ChType::Array(_) => PyList::empty(py).into_any().unbind(),
        ChType::Map(..) => PyDict::new(py).into_any().unbind(),
        ChType::Json { .. } => PyDict::new(py).into_any().unbind(),
        ChType::Tuple(elements) => {
            let defaults = elements
                .iter()
                .map(|(_, element_type)| default_pyobject(py, element_type))
                .collect::<PyResult<Vec<_>>>()?;
            PyTuple::new(py, defaults)?.into_any().unbind()
        }
        ChType::AggregateFunction { .. } => {
            return Err(PyNotImplementedError::new_err(
                "null Nullable(Tuple(...)) rows containing AggregateFunction are unsupported until the core provides a canonical placeholder state",
            ));
        }
        // Every remaining scalar accepts a raw 0 (epoch units, enum code,
        // integer UUID/IP forms, Decimal("0")).
        _ => 0i64.into_pyobject(py)?.into_any().unbind(),
    })
}
