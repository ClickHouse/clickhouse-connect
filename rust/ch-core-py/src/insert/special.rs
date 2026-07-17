use super::*;

/// Build Nothing from a Python column without constructing per-row scalar
/// values. Plain Nothing ignores every placeholder, matching the Python
/// codec. Nullable(Nothing) additionally retains which placeholders were
/// Python None so the core can emit the structural null map before the
/// canonical Nothing marker bytes.
pub(super) fn build_nothing_column(
    name: &str,
    values: &Bound<'_, PyAny>,
    row_count: usize,
    nullable: bool,
) -> PyResult<Column> {
    let column_values = ColumnValues::new(values, name)?;
    check_row_count(name, &column_values, row_count)?;

    if let Ok(list) = values.downcast_exact::<PyList>() {
        return Ok(nothing_column_from_seq(&ListSeq(list), row_count, nullable));
    }
    if let Ok(tuple) = values.downcast_exact::<PyTuple>() {
        return Ok(nothing_column_from_seq(
            &TupleSeq(tuple),
            row_count,
            nullable,
        ));
    }

    let validity = if nullable {
        let mut null_map = Vec::with_capacity(row_count);
        for row in 0..row_count {
            null_map.push(u8::from(column_values.get_item(row)?.is_none()));
        }
        Some(Bitmap::from_ch_null_map(&null_map))
    } else {
        None
    };
    Ok(nothing_column(row_count, validity))
}

pub(super) fn nothing_column_from_seq<S: FastSeq>(
    seq: &S,
    row_count: usize,
    nullable: bool,
) -> Column {
    // The unsafe seq reads below are in bounds only under this equality.
    assert_eq!(seq.size(), row_count);
    let validity = nullable.then(|| {
        let mut null_map = Vec::with_capacity(row_count);
        for row in 0..row_count {
            // SAFETY: row < row_count == seq.size(), the sequence is borrowed
            // while the GIL is held, and comparing to Py_None runs no Python.
            let value = unsafe { seq.get(row) };
            null_map.push(u8::from(value == unsafe { ffi::Py_None() }));
        }
        Bitmap::from_ch_null_map(&null_map)
    });
    nothing_column(row_count, validity)
}

fn nothing_column(row_count: usize, validity: Option<Bitmap>) -> Column {
    Column::Nothing(match validity {
        Some(validity) => NothingColumn::new_nullable(row_count, validity),
        None => NothingColumn::new(row_count),
    })
}

/// Build one AggregateFunction state column from exact serialized state bytes.
/// State framing and validation are function-specific, so the binding only
/// recovers Python buffer boundaries; the core encoder validates every slice
/// against the codec registered for the declared ChType.
pub(super) fn build_aggregate_state_column(
    py: Python<'_>,
    name: &str,
    ch_type: &ChType,
    values: &Bound<'_, PyAny>,
    row_count: usize,
) -> PyResult<Column> {
    let column_values = ColumnValues::new(values, name)?;
    check_row_count(name, &column_values, row_count)?;
    if let Ok(list) = values.downcast_exact::<PyList>() {
        return aggregate_state_column_from_seq(py, name, ch_type, &ListSeq(list), row_count);
    }
    if let Ok(tuple) = values.downcast_exact::<PyTuple>() {
        return aggregate_state_column_from_seq(py, name, ch_type, &TupleSeq(tuple), row_count);
    }
    aggregate_state_column_from_rows(name, ch_type, &column_values, row_count)
}

fn aggregate_state_column_from_rows<'py, R: RowAccess<'py>>(
    name: &str,
    ch_type: &ChType,
    rows: &R,
    row_count: usize,
) -> PyResult<Column> {
    let mut offsets = Vec::with_capacity(row_count + 1);
    let mut data = Vec::new();
    offsets.push(0);
    for row in 0..row_count {
        let value = rows.value(row)?;
        append_aggregate_state(&value, name, ch_type, row, &mut offsets, &mut data)?;
    }
    Ok(Column::AggregateState(AggregateStateColumn::new(
        offsets, data,
    )))
}

pub(super) fn aggregate_state_column_from_seq<S: FastSeq>(
    py: Python<'_>,
    name: &str,
    ch_type: &ChType,
    seq: &S,
    row_count: usize,
) -> PyResult<Column> {
    let mut offsets = Vec::with_capacity(row_count + 1);
    let mut data = Vec::with_capacity(aggregate_state_size_hint(py, seq, row_count));
    offsets.push(0);
    for row in 0..row_count {
        // SAFETY: row is in bounds; the size is checked against row_count up
        // front and revalidated after any row that ran Python.
        // `from_borrowed_ptr` takes a strong reference, so container mutation
        // during generic buffer conversion cannot invalidate `value`.
        let value = unsafe { Bound::from_borrowed_ptr(py, seq.get(row)) };
        let ran_python =
            append_aggregate_state(&value, name, ch_type, row, &mut offsets, &mut data)?;
        if ran_python {
            // Dropping the strong reference can run __del__, which can resize
            // the container, so it must precede the size revalidation.
            drop(value);
            check_not_resized(seq, name, row_count)?;
        }
    }
    Ok(Column::AggregateState(AggregateStateColumn::new(
        offsets, data,
    )))
}

/// Exact data size when every row is exact bytes, read via `Py_SIZE` without
/// running Python; 0 (no reservation) otherwise.
fn aggregate_state_size_hint<S: FastSeq>(_py: Python<'_>, seq: &S, row_count: usize) -> usize {
    let mut total = 0usize;
    for row in 0..row_count {
        // SAFETY: the GIL is held, row is in bounds, and no Python runs in
        // this loop, so the borrowed pointer stays valid.
        let ptr = unsafe { seq.get(row) };
        if unsafe { ffi::PyBytes_CheckExact(ptr) } == 0 {
            return 0;
        }
        total = total.saturating_add(unsafe { ffi::Py_SIZE(ptr) } as usize);
    }
    total
}

fn agg_offset_overflow(name: &str) -> PyErr {
    PyValueError::new_err(format!(
        "column {name:?} AggregateFunction state data exceeds i64 offset capacity"
    ))
}

fn agg_state_convert_err(py: Python<'_>, name: &str, row: usize, err: PyErr) -> PyErr {
    let wrapped = PyValueError::new_err(format!(
        "column {name:?} row {row} cannot be converted to AggregateFunction state bytes"
    ));
    wrapped.set_cause(py, Some(err));
    wrapped
}

/// Append one Python bytes-like state directly into the column data run.
/// Returns true when generic buffer conversion may have executed Python.
fn append_aggregate_state(
    value: &Bound<'_, PyAny>,
    name: &str,
    ch_type: &ChType,
    row: usize,
    offsets: &mut Vec<i64>,
    data: &mut Vec<u8>,
) -> PyResult<bool> {
    if value.is_none() {
        return Err(PyValueError::new_err(format!(
            "column {name:?} row {row} is None but {ch_type} is not Nullable"
        )));
    }
    let ran_python = if let Ok(bytes) = value.downcast::<PyBytes>() {
        data.extend_from_slice(bytes.as_bytes());
        false
    } else if let Ok(bytes) = value.downcast::<PyByteArray>() {
        // SAFETY: the GIL is held and extend_from_slice copies the complete
        // buffer before any Python or PyO3 API can run and invalidate it.
        data.extend_from_slice(unsafe { bytes.as_bytes() });
        false
    } else {
        let buffer = PyBuffer::<u8>::get(value)
            .map_err(|err| agg_state_convert_err(value.py(), name, row, err))?;
        let start = data.len();
        let end = start
            .checked_add(buffer.item_count())
            .ok_or_else(|| agg_offset_overflow(name))?;
        data.resize(end, 0);
        if let Err(err) = buffer.copy_to_slice(value.py(), &mut data[start..end]) {
            data.truncate(start);
            return Err(agg_state_convert_err(value.py(), name, row, err));
        }
        true
    };
    offsets.push(i64::try_from(data.len()).map_err(|_| agg_offset_overflow(name))?);
    Ok(ran_python)
}

pub(super) fn build_low_cardinality_column(
    py: Python<'_>,
    name: &str,
    inner: &ChType,
    values: &Bound<'_, PyAny>,
    row_count: usize,
) -> PyResult<Column> {
    // Resolve the physical dict value type: strip the SAF chain, unwrap an
    // optional Nullable, strip again. So LowCardinality(SAF(anyLast, String)) and
    // LowCardinality(SAF(anyLast, Nullable(String))) reach the String path.
    let (nullable, value_type) = low_cardinality_dict_value_type(inner);

    if !is_low_cardinality_inner(value_type) {
        return Err(PyNotImplementedError::new_err(format!(
            "unsupported LowCardinality inner type {value_type} for column {name:?}"
        )));
    }

    let column_values = ColumnValues::new(values, name)?;
    check_row_count(name, &column_values, row_count)?;
    if wide_int_layout(value_type).is_some() {
        return lc_wide_column(py, name, value_type, &column_values, row_count, nullable);
    }
    if matches!(value_type, ChType::String) {
        if let Some(column) =
            lc_string_fast_column(py, name, value_type, values, row_count, nullable)?
        {
            return Ok(column);
        }
    }
    lc_scalar_column(py, name, value_type, &column_values, row_count, nullable)
}

pub(super) fn lc_scalar_column<'py, R: RowAccess<'py>>(
    py: Python<'py>,
    name: &str,
    value_type: &ChType,
    rows: &R,
    row_count: usize,
    nullable: bool,
) -> PyResult<Column> {
    let mut indices = Vec::with_capacity(row_count);
    let mut dict_values = Vec::new();
    let mut slots = HashMap::<ScalarKey, i32>::new();
    let mut null_map = nullable.then(|| Vec::with_capacity(row_count));
    // Time64 is rejected by is_low_cardinality_inner, so only Time probes.
    let mut time_probe = match value_type {
        ChType::Time => TimeScalarProbe::new(value_type),
        _ => None,
    };

    if nullable && row_count > 0 {
        dict_values.push(default_scalar(value_type)?);
    }

    for row in 0..row_count {
        let value = rows.value(row)?;
        if value.is_none() {
            if !nullable {
                return Err(PyValueError::new_err(format!(
                    "column {name:?} row {row} is None but LowCardinality({value_type}) is not nullable"
                )));
            }
            indices.push(0);
            if let Some(nulls) = &mut null_map {
                nulls.push(1);
            }
            continue;
        }

        let time_hit = match time_probe.as_mut() {
            Some(probe) => probe.probe(&value, name, row)?,
            None => None,
        };
        if let Some(TimeProbe::Nat) = time_hit {
            if !nullable {
                return Err(PyValueError::new_err(format!(
                    "column {name:?} row {row} is NaT but LowCardinality({value_type}) is not nullable"
                )));
            }
            indices.push(0);
            if let Some(nulls) = &mut null_map {
                nulls.push(1);
            }
            continue;
        }

        if let Some(nulls) = &mut null_map {
            nulls.push(0);
        }
        let scalar = match time_hit {
            Some(TimeProbe::Ticks(ticks)) => time_ticks_scalar(value_type, ticks, name, row)?,
            _ => convert_scalar(py, value_type, &value, name, row)?,
        };
        let key = scalar.key();
        let slot = if let Some(slot) = slots.get(&key) {
            *slot
        } else {
            let slot = i32::try_from(dict_values.len()).map_err(|_| {
                PyValueError::new_err(format!(
                    "column {name:?} LowCardinality dictionary exceeds i32 index capacity"
                ))
            })?;
            dict_values.push(scalar);
            slots.insert(key, slot);
            slot
        };
        indices.push(slot);
    }

    let dict_column = column_from_scalars(value_type, dict_values, None)?;
    match null_map {
        Some(nulls) => Ok(Column::Dictionary(DictionaryColumn::new_nullable(
            indices,
            dict_column,
            Bitmap::from_ch_null_map(&nulls),
        ))),
        None => Ok(Column::Dictionary(DictionaryColumn::new(
            indices,
            dict_column,
        ))),
    }
}

/// Build LowCardinality over a wide integer without a heap allocation per
/// input cell. Conversion writes into a fixed stack buffer; only distinct
/// dictionary entries are retained, and the final dictionary data is copied
/// once into the core's contiguous fixed-binary representation.
pub(super) fn lc_wide_column<'py, R: RowAccess<'py>>(
    py: Python<'py>,
    name: &str,
    value_type: &ChType,
    rows: &R,
    row_count: usize,
    nullable: bool,
) -> PyResult<Column> {
    let (width, signed, type_name) = wide_int_layout(value_type)
        .ok_or_else(|| PyValueError::new_err("internal wide integer type mismatch"))?;
    let mut indices = Vec::with_capacity(row_count);
    let mut dict_values = Vec::<[u8; 32]>::new();
    let mut slots = HashMap::<[u8; 32], i32>::with_capacity(row_count.min(1024));
    let mut null_map = nullable.then(|| Vec::with_capacity(row_count));

    if nullable && row_count > 0 {
        dict_values.push([0u8; 32]);
    }

    for row in 0..row_count {
        let value = rows.value(row)?;
        if value.is_none() {
            if !nullable {
                return Err(PyValueError::new_err(format!(
                    "column {name:?} row {row} is None but LowCardinality({value_type}) is not nullable"
                )));
            }
            indices.push(0);
            if let Some(nulls) = &mut null_map {
                nulls.push(1);
            }
            continue;
        }
        if let Some(nulls) = &mut null_map {
            nulls.push(0);
        }

        let mut bytes = [0u8; 32];
        wide_int_into(
            py,
            &value,
            &mut bytes[..width],
            signed,
            name,
            row,
            type_name,
        )?;
        let slot = match slots.entry(bytes) {
            Entry::Occupied(slot) => *slot.get(),
            Entry::Vacant(vacant) => {
                let slot = i32::try_from(dict_values.len()).map_err(|_| {
                    PyValueError::new_err(format!(
                        "column {name:?} LowCardinality dictionary exceeds i32 index capacity"
                    ))
                })?;
                dict_values.push(bytes);
                *vacant.insert(slot)
            }
        };
        indices.push(slot);
    }

    let byte_len = width.checked_mul(dict_values.len()).ok_or_else(|| {
        PyValueError::new_err(format!(
            "column {name:?} wide integer dictionary byte size exceeds usize capacity"
        ))
    })?;
    let mut data = Vec::with_capacity(byte_len);
    for value in dict_values {
        data.extend_from_slice(&value[..width]);
    }
    let dict_column = finish_wide_int_column(value_type, data, None)?;
    match null_map {
        Some(nulls) => Ok(Column::Dictionary(DictionaryColumn::new_nullable(
            indices,
            dict_column,
            Bitmap::from_ch_null_map(&nulls),
        ))),
        None => Ok(Column::Dictionary(DictionaryColumn::new(
            indices,
            dict_column,
        ))),
    }
}

/// LowCardinality(String) fast path over an exact list or tuple. Returns
/// `Ok(None)` for other containers; the caller falls through to the generic
/// scalar loop.
fn lc_string_fast_column(
    py: Python<'_>,
    name: &str,
    value_type: &ChType,
    values: &Bound<'_, PyAny>,
    row_count: usize,
    nullable: bool,
) -> PyResult<Option<Column>> {
    if let Ok(list) = values.downcast_exact::<PyList>() {
        return lc_string_seq(py, name, value_type, &ListSeq(list), row_count, nullable).map(Some);
    }
    if let Ok(tuple) = values.downcast_exact::<PyTuple>() {
        return lc_string_seq(py, name, value_type, &TupleSeq(tuple), row_count, nullable)
            .map(Some);
    }
    Ok(None)
}

/// Build a LowCardinality(String) dictionary column with two cache levels:
/// repeated str objects hit a pointer-identity map (no content read at all),
/// and new exact-str objects read their UTF-8 once for a content-keyed map,
/// allocating only when the content is genuinely new to the dictionary.
/// Non-str values fall back to `convert_scalar` for identical accepted-type
/// and error semantics; that fallback can run arbitrary Python (exotic buffer
/// types), so it holds a strong reference and revalidates the container size.
/// Pointer identity is only meaningful while no Python code has run since the
/// key was cached: a list can drop the last reference to an already-scanned
/// str mid-call, and the allocator can hand its address to a new str, so
/// every fallback return clears the pointer cache. Entries cached after the
/// clear are valid until the next fallback. A tuple keeps every item alive
/// for the whole call, so its cache never needs clearing.
pub(super) fn lc_string_seq<S: FastSeq>(
    py: Python<'_>,
    name: &str,
    value_type: &ChType,
    seq: &S,
    row_count: usize,
    nullable: bool,
) -> PyResult<Column> {
    let mut indices = Vec::with_capacity(row_count);
    let mut dict_values: Vec<Scalar> = Vec::new();
    let mut ptr_slots: HashMap<usize, i32, std::hash::BuildHasherDefault<PtrHasher>> =
        HashMap::default();
    let mut content_slots: HashMap<Vec<u8>, i32> = HashMap::new();
    let mut null_map = nullable.then(|| Vec::with_capacity(row_count));

    if nullable && row_count > 0 {
        dict_values.push(default_scalar(value_type)?);
    }

    let dict_slot = |dict_values: &Vec<Scalar>| {
        i32::try_from(dict_values.len()).map_err(|_| {
            PyValueError::new_err(format!(
                "column {name:?} LowCardinality dictionary exceeds i32 index capacity"
            ))
        })
    };

    for row in 0..row_count {
        // SAFETY: row < row_count, the container size the caller checked and
        // every fallback revalidates; the borrowed pointer is consumed before
        // any Python code can run.
        let ptr = unsafe { seq.get(row) };
        if ptr == unsafe { ffi::Py_None() } {
            let Some(null_map) = &mut null_map else {
                return Err(PyValueError::new_err(format!(
                    "column {name:?} row {row} is None but LowCardinality({value_type}) is not nullable"
                )));
            };
            indices.push(0);
            null_map.push(1);
            continue;
        }
        if let Some(null_map) = &mut null_map {
            null_map.push(0);
        }
        if let Some(&slot) = ptr_slots.get(&(ptr as usize)) {
            indices.push(slot);
            continue;
        }
        let slot = if unsafe { ffi::PyUnicode_CheckExact(ptr) } != 0 {
            // SAFETY: ptr is a valid borrowed reference, verified an exact
            // str; reading its UTF-8 runs no Python code.
            let obj =
                unsafe { Bound::from_borrowed_ptr(py, ptr).downcast_into_unchecked::<PyString>() };
            let bytes = obj.to_str()?.as_bytes();
            let slot = match content_slots.get(bytes) {
                Some(&slot) => slot,
                None => {
                    let slot = dict_slot(&dict_values)?;
                    content_slots.insert(bytes.to_vec(), slot);
                    dict_values.push(Scalar::Bytes(bytes.to_vec()));
                    slot
                }
            };
            // The container still holds this item (no Python ran since the
            // borrowed read), so its address is stable and unique among the
            // column's live values.
            if ptr_slots.len() < PTR_CACHE_CAP {
                ptr_slots.insert(ptr as usize, slot);
            }
            slot
        } else {
            // SAFETY: ptr is valid here; the strong reference keeps the item
            // alive across any Python code the fallback runs.
            let obj = unsafe { Bound::from_borrowed_ptr(py, ptr) };
            let scalar = convert_scalar(py, value_type, &obj, name, row)?;
            if S::MUTABLE {
                // The fallback may have run Python code: a same-size item
                // replacement can free a cached str whose address the
                // allocator may reuse, so drop all pointer-identity entries.
                check_not_resized(seq, name, row_count)?;
                ptr_slots.clear();
            }
            let Scalar::Bytes(bytes) = scalar else {
                return Err(PyValueError::new_err("internal scalar type mismatch"));
            };
            match content_slots.get(bytes.as_slice()) {
                Some(&slot) => slot,
                None => {
                    let slot = dict_slot(&dict_values)?;
                    content_slots.insert(bytes.clone(), slot);
                    dict_values.push(Scalar::Bytes(bytes));
                    slot
                }
            }
        };
        indices.push(slot);
    }

    let dict_column = column_from_scalars(value_type, dict_values, None)?;
    Ok(match null_map {
        Some(nulls) => Column::Dictionary(DictionaryColumn::new_nullable(
            indices,
            dict_column,
            Bitmap::from_ch_null_map(&nulls),
        )),
        None => Column::Dictionary(DictionaryColumn::new(indices, dict_column)),
    })
}

/// Build a UUID column: exact `uuid.UUID` items read the `int` attribute and
/// write the 16 wire bytes (hi u64 LE, then lo u64 LE) straight into the
/// column buffer; anything else (str, int, bytes) falls back to
/// `convert_scalar`. Both the attribute read (a patched class attribute) and
/// the fallback can run Python code, so the container size is revalidated
/// after each before the next borrowed read.
pub(super) fn uuid_seq<S: FastSeq>(
    py: Python<'_>,
    seq: &S,
    ch_type: &ChType,
    name: &str,
    row_count: usize,
    nullable: bool,
) -> PyResult<Column> {
    let uuid_type = py
        .import(intern!(py, "uuid"))?
        .getattr(intern!(py, "UUID"))?;
    let uuid_type_ptr = uuid_type.as_ptr();
    let int_attr = intern!(py, "int");
    let mut data = Vec::with_capacity(16 * row_count);
    let mut null_map = nullable.then(|| Vec::with_capacity(row_count));

    for row in 0..row_count {
        // SAFETY: row < row_count, the container size the caller checked and
        // every conversion revalidates; the borrowed pointer is consumed
        // before any Python code can run.
        let ptr = unsafe { seq.get(row) };
        if ptr == unsafe { ffi::Py_None() } {
            let Some(null_map) = &mut null_map else {
                return Err(PyValueError::new_err(format!(
                    "column {name:?} row {row} is None but {ch_type} is not Nullable"
                )));
            };
            null_map.push(1);
            data.extend_from_slice(&[0u8; 16]);
            continue;
        }
        if let Some(null_map) = &mut null_map {
            null_map.push(0);
        }
        // SAFETY: ptr is valid here; the strong reference keeps the item
        // alive across any Python code the conversion runs.
        let obj = unsafe { Bound::from_borrowed_ptr(py, ptr) };
        let fast = if unsafe { ffi::Py_TYPE(ptr) }.cast::<ffi::PyObject>() == uuid_type_ptr {
            obj.getattr(int_attr).and_then(|i| i.extract::<u128>()).ok()
        } else {
            None
        };
        match fast {
            Some(v) => {
                data.extend_from_slice(&((v >> 64) as u64).to_le_bytes());
                data.extend_from_slice(&(v as u64).to_le_bytes());
            }
            None => {
                let Scalar::Bytes(bytes) = convert_scalar(py, ch_type, &obj, name, row)? else {
                    return Err(PyValueError::new_err("internal scalar type mismatch"));
                };
                data.extend_from_slice(&bytes);
            }
        }
        check_not_resized(seq, name, row_count)?;
    }

    Ok(Column::Uuid(match null_map {
        Some(nulls) => FixedBinaryColumn::new_nullable(data, 16, Bitmap::from_ch_null_map(&nulls)),
        None => FixedBinaryColumn::new(data, 16),
    }))
}

/// Build an IPv4 column: exact `ipaddress.IPv4Address` items read the `_ip`
/// slot directly (or the attribute when the slot offset cannot be resolved),
/// exact ints and strs convert without constructing exceptions, and anything
/// else falls back to `convert_scalar`. The attribute read (a patched class
/// attribute) and the fallback can run Python code, so the container size is
/// revalidated after each before the next borrowed read.
pub(super) fn ipv4_seq<S: FastSeq>(
    py: Python<'_>,
    seq: &S,
    ch_type: &ChType,
    name: &str,
    row_count: usize,
    nullable: bool,
) -> PyResult<Column> {
    let ipv4_type = py
        .import(intern!(py, "ipaddress"))?
        .getattr(intern!(py, "IPv4Address"))?;
    let ipv4_type_ptr = ipv4_type.as_ptr();
    let ip_attr = intern!(py, "_ip");
    // Re-resolved after any edge that runs Python, which can patch the class.
    let mut ip_slot = slot_object_offset(&ipv4_type, ip_attr);
    let mut values = Vec::<u32>::with_capacity(row_count);
    let mut null_map = nullable.then(|| Vec::with_capacity(row_count));

    for row in 0..row_count {
        // SAFETY: row < row_count, the container size the caller checked and
        // every conversion revalidates; the borrowed pointer is consumed
        // before any Python code can run.
        let ptr = unsafe { seq.get(row) };
        if ptr == unsafe { ffi::Py_None() } {
            let Some(null_map) = &mut null_map else {
                return Err(PyValueError::new_err(format!(
                    "column {name:?} row {row} is None but {ch_type} is not Nullable"
                )));
            };
            null_map.push(1);
            values.push(0);
            continue;
        }
        if let Some(null_map) = &mut null_map {
            null_map.push(0);
        }
        if unsafe { ffi::Py_TYPE(ptr) }.cast::<ffi::PyObject>() == ipv4_type_ptr {
            if let Some(offset) = ip_slot {
                // SAFETY: ptr is an exact instance of the class the offset
                // was resolved from; the slot holds a strong reference (kept
                // alive by the instance) or NULL, and reading it runs no
                // Python code.
                let slot = unsafe { *ptr.cast::<u8>().offset(offset).cast::<*mut ffi::PyObject>() };
                if !slot.is_null() {
                    // SAFETY: GIL held; slot is a valid object pointer.
                    if let Ok(v) = unsafe { <u32 as FastValue>::from_exact(slot, ch_type, 0) } {
                        values.push(v);
                        continue;
                    }
                }
            }
            // SAFETY: ptr is valid here; the strong reference keeps the item
            // alive across any Python code the conversion runs.
            let obj = unsafe { Bound::from_borrowed_ptr(py, ptr) };
            match obj.getattr(ip_attr).and_then(|o| o.extract::<u32>()) {
                Ok(v) => values.push(v),
                Err(_) => values.push(ipv4_fallback(py, ch_type, &obj, name, row)?),
            }
            check_not_resized(seq, name, row_count)?;
            ip_slot = slot_object_offset(&ipv4_type, ip_attr);
            continue;
        }
        // SAFETY: GIL held; ptr is a valid borrowed item pointer.
        if let Ok(v) = unsafe { <u32 as FastValue>::from_exact(ptr, ch_type, 0) } {
            values.push(v);
            continue;
        }
        if unsafe { ffi::PyUnicode_CheckExact(ptr) } != 0 {
            // SAFETY: ptr is a valid borrowed reference, verified an exact
            // str; reading its UTF-8 runs no Python code.
            let obj =
                unsafe { Bound::from_borrowed_ptr(py, ptr).downcast_into_unchecked::<PyString>() };
            let v = obj
                .to_str()?
                .parse::<Ipv4Addr>()
                .map(|addr| u32::from_be_bytes(addr.octets()))
                .map_err(|_| conversion_error(name, row, "IPv4"))?;
            values.push(v);
            continue;
        }
        // SAFETY: ptr is valid here; the strong reference keeps the item
        // alive across any Python code the fallback runs.
        let obj = unsafe { Bound::from_borrowed_ptr(py, ptr) };
        values.push(ipv4_fallback(py, ch_type, &obj, name, row)?);
        check_not_resized(seq, name, row_count)?;
        ip_slot = slot_object_offset(&ipv4_type, ip_attr);
    }

    Ok(Column::Ipv4(match null_map {
        Some(nulls) => PrimitiveColumn::new_nullable(values, Bitmap::from_ch_null_map(&nulls)),
        None => PrimitiveColumn::new(values),
    }))
}

/// Instance offset of `attr` when it resolves to a plain object slot
/// (`__slots__` member descriptor, `Py_T_OBJECT_EX`, no flags) defined on
/// exactly `class`. Lets a loop over exact instances read the slot directly,
/// like CPython's LOAD_ATTR_SLOT specialization. `None` (patched or exotic
/// class attribute) means callers must use a normal attribute read.
fn slot_object_offset(class: &Bound<'_, PyAny>, attr: &Bound<'_, PyString>) -> Option<isize> {
    let descr = class.getattr(attr).ok()?;
    if unsafe { ffi::Py_TYPE(descr.as_ptr()) != std::ptr::addr_of_mut!(ffi::PyMemberDescr_Type) } {
        return None;
    }
    // SAFETY: descr is verified an exact member_descriptor, so its layout is
    // PyMemberDescrObject; d_member points at the defining PyMemberDef (the
    // ffi binding mistypes the field, hence the cast).
    unsafe {
        let descr = descr.as_ptr().cast::<ffi::PyMemberDescrObject>();
        if (*descr).d_common.d_type.cast::<ffi::PyObject>() != class.as_ptr() {
            return None;
        }
        let member = (*descr).d_member.cast::<ffi::PyMemberDef>();
        if member.is_null() || (*member).name.is_null() {
            return None;
        }
        if (*member).type_code != ffi::Py_T_OBJECT_EX || (*member).flags != 0 {
            return None;
        }
        let offset = (*member).offset;
        (offset > 0).then_some(offset)
    }
}

fn ipv4_fallback(
    py: Python<'_>,
    ch_type: &ChType,
    obj: &Bound<'_, PyAny>,
    name: &str,
    row: usize,
) -> PyResult<u32> {
    match convert_scalar(py, ch_type, obj, name, row)? {
        Scalar::Ipv4(v) => Ok(v),
        _ => Err(PyValueError::new_err("internal scalar type mismatch")),
    }
}

/// Enum code width plumbing for the enum sequence fast path.
pub(super) trait EnumCode: Copy + Default {
    const TYPE_NAME: &'static str;

    fn from_enum_scalar(scalar: Scalar) -> PyResult<Self>;

    fn into_enum_column(values: Vec<Self>, validity: Option<Bitmap>) -> Column;
}

macro_rules! impl_enum_code {
    ($ty:ty, $variant:ident, $type_name:literal) => {
        impl EnumCode for $ty {
            const TYPE_NAME: &'static str = $type_name;

            fn from_enum_scalar(scalar: Scalar) -> PyResult<Self> {
                match scalar {
                    Scalar::$variant(value) => Ok(value),
                    _ => Err(PyValueError::new_err("internal scalar type mismatch")),
                }
            }

            fn into_enum_column(values: Vec<Self>, validity: Option<Bitmap>) -> Column {
                Column::$variant(match validity {
                    Some(validity) => PrimitiveColumn::new_nullable(values, validity),
                    None => PrimitiveColumn::new(values),
                })
            }
        }
    };
}

impl_enum_code!(i8, Enum8, "Enum8");
impl_enum_code!(i16, Enum16, "Enum16");

/// Build an Enum column with two lookup levels, following `lc_string_seq`:
/// repeated str objects hit a pointer-identity map, and new exact-str objects
/// read their UTF-8 once for a content lookup against the variant labels. An
/// exact str whose label is not defined is an error; anything else (raw int
/// codes, str subclasses) falls back to `convert_scalar`. The fallback can
/// run arbitrary Python, so it revalidates the container size and clears the
/// pointer-identity cache, whose entries are only valid while no Python code
/// has run since they were cached.
pub(super) fn enum_seq<C: EnumCode, S: FastSeq>(
    py: Python<'_>,
    seq: &S,
    ch_type: &ChType,
    variants: &[(String, C)],
    name: &str,
    row_count: usize,
    nullable: bool,
) -> PyResult<Column> {
    let mut content_codes: HashMap<&[u8], C> = HashMap::with_capacity(variants.len());
    for (label, code) in variants {
        // First definition wins, matching enum8_value's linear scan.
        content_codes.entry(label.as_bytes()).or_insert(*code);
    }
    let mut ptr_codes: HashMap<usize, C, std::hash::BuildHasherDefault<PtrHasher>> =
        HashMap::default();
    let mut codes = Vec::with_capacity(row_count);
    let mut null_map = nullable.then(|| Vec::with_capacity(row_count));

    for row in 0..row_count {
        // SAFETY: row < row_count, the container size the caller checked and
        // every fallback revalidates; the borrowed pointer is consumed before
        // any Python code can run.
        let ptr = unsafe { seq.get(row) };
        if ptr == unsafe { ffi::Py_None() } {
            let Some(null_map) = &mut null_map else {
                return Err(PyValueError::new_err(format!(
                    "column {name:?} row {row} is None but {ch_type} is not Nullable"
                )));
            };
            null_map.push(1);
            codes.push(C::default());
            continue;
        }
        if let Some(null_map) = &mut null_map {
            null_map.push(0);
        }
        if let Some(&code) = ptr_codes.get(&(ptr as usize)) {
            codes.push(code);
            continue;
        }
        if unsafe { ffi::PyUnicode_CheckExact(ptr) } != 0 {
            // SAFETY: ptr is a valid borrowed reference, verified an exact
            // str; reading its UTF-8 runs no Python code.
            let obj =
                unsafe { Bound::from_borrowed_ptr(py, ptr).downcast_into_unchecked::<PyString>() };
            let label = obj.to_str()?;
            let Some(&code) = content_codes.get(label.as_bytes()) else {
                return Err(PyValueError::new_err(format!(
                    "column {name:?} row {row} {} label {label:?} is not defined",
                    C::TYPE_NAME
                )));
            };
            // The container still holds this item (no Python ran since the
            // borrowed read), so its address is stable and unique among the
            // column's live values.
            if ptr_codes.len() < PTR_CACHE_CAP {
                ptr_codes.insert(ptr as usize, code);
            }
            codes.push(code);
            continue;
        }
        // SAFETY: ptr is valid here; the strong reference keeps the item
        // alive across any Python code the fallback runs.
        let obj = unsafe { Bound::from_borrowed_ptr(py, ptr) };
        let scalar = convert_scalar(py, ch_type, &obj, name, row)?;
        codes.push(C::from_enum_scalar(scalar)?);
        if S::MUTABLE {
            check_not_resized(seq, name, row_count)?;
            // The fallback may have freed a cached str whose address the
            // allocator can reuse, so drop all pointer-identity entries.
            ptr_codes.clear();
        }
    }

    Ok(C::into_enum_column(
        codes,
        null_map.map(|nulls| Bitmap::from_ch_null_map(&nulls)),
    ))
}
