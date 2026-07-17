use super::*;

/// The column's validity bitmap, if any.
pub(super) fn column_validity(col: &Column) -> Option<&Bitmap> {
    match col {
        Column::Bool(c) => c.validity.as_ref(),
        Column::Int8(c) => c.validity.as_ref(),
        Column::Int16(c) => c.validity.as_ref(),
        Column::Int32(c) => c.validity.as_ref(),
        Column::Int64(c) => c.validity.as_ref(),
        Column::UInt8(c) => c.validity.as_ref(),
        Column::UInt16(c) => c.validity.as_ref(),
        Column::UInt32(c) => c.validity.as_ref(),
        Column::UInt64(c) => c.validity.as_ref(),
        Column::Float32(c) => c.validity.as_ref(),
        Column::Float64(c) => c.validity.as_ref(),
        Column::BFloat16(c) => c.validity.as_ref(),
        Column::QBit(c) => c.validity.as_ref(),
        Column::Nothing(c) => c.validity.as_ref(),
        Column::Date(c) => c.validity.as_ref(),
        Column::Date32(c) => c.validity.as_ref(),
        Column::DateTime(c) => c.validity.as_ref(),
        Column::DateTime64(c) => c.validity.as_ref(),
        Column::Time(c) => c.validity.as_ref(),
        Column::Time64(c) => c.validity.as_ref(),
        Column::Interval(c) => c.validity.as_ref(),
        Column::Utf8(c) => c.validity.as_ref(),
        Column::AggregateState(_) => None,
        Column::FixedBinary(c) => c.validity.as_ref(),
        Column::Ipv4(c) => c.validity.as_ref(),
        Column::Ipv6(c) => c.validity.as_ref(),
        Column::Uuid(c) => c.validity.as_ref(),
        Column::Enum8(c) => c.validity.as_ref(),
        Column::Enum16(c) => c.validity.as_ref(),
        Column::Int128(c) => c.validity.as_ref(),
        Column::UInt128(c) => c.validity.as_ref(),
        Column::Int256(c) => c.validity.as_ref(),
        Column::UInt256(c) => c.validity.as_ref(),
        // A LowCardinality column's nulls live in the index validity, the Arrow
        // dictionary convention, not as a dictionary entry.
        Column::Dictionary(c) => c.validity.as_ref(),
        Column::Decimal(c) => c.validity.as_ref(),
        // Arrays carry no array-level validity; element nulls live on `values`.
        Column::Array(_) => None,
        // A Nullable(Tuple) carries tuple-level validity here; element nulls
        // live on the field columns. A plain Tuple has `validity == None`.
        Column::Tuple(c) => c.validity.as_ref(),
        // Dynamic has intrinsic NULL rows in its dense-union routing buffers,
        // not a top-level validity bitmap.
        Column::Dynamic(_) => None,
        // Maps are never nullable at the map level; value nulls live on the
        // values column inside `entries`.
        Column::Map(_) => None,
        // Variant has intrinsic NULL rows in its dense-union routing buffers,
        // not a top-level validity bitmap.
        Column::Variant(_) => None,
        // JSON only carries top-level validity under Nullable(JSON).
        Column::Json(c) => c.validity.as_ref(),
    }
}

/// Widen one exact little-endian ClickHouse BFloat16 word to Float32.
#[inline]
pub(super) fn bfloat16_to_f32(word: [u8; 2]) -> f32 {
    f32::from_bits(u32::from(u16::from_le_bytes(word)) << 16)
}

/// Tight per-value loop for one primitive column: `make` is the per-value FFI
/// constructor, resolved once by the caller's variant dispatch rather than per
/// cell. The nullable arm checks the bitmap per cell but keeps the single
/// dispatch.
///
/// # Safety
///
/// Requires the GIL. `make` must return an owned reference or null; each
/// pointer passed to `sink` is an owned reference the sink must take over
/// exactly once.
unsafe fn fill_prim<T, F, S>(
    py: Python<'_>,
    values: &[T],
    validity: Option<&Bitmap>,
    make: F,
    sink: &mut S,
) -> PyResult<()>
where
    T: Copy,
    F: Fn(T) -> *mut ffi::PyObject,
    S: FnMut(usize, *mut ffi::PyObject),
{
    match validity {
        None => {
            for (i, &v) in values.iter().enumerate() {
                let item = make(v);
                if item.is_null() {
                    return Err(PyErr::fetch(py));
                }
                sink(i, item);
            }
        }
        Some(bm) => {
            for (i, &v) in values.iter().enumerate() {
                let item = if bm.is_valid(i) {
                    let made = make(v);
                    if made.is_null() {
                        return Err(PyErr::fetch(py));
                    }
                    made
                } else {
                    none_owned_ptr()
                };
                sink(i, item);
            }
        }
    }
    Ok(())
}

/// Tight per-cell loop for a column whose constructor works by row index:
/// `make` is the per-cell builder, with its ctx lookups hoisted by the caller.
/// The nullable arm checks the bitmap per cell but keeps the single dispatch.
///
/// # Safety
///
/// Requires the GIL. `make` must return an owned reference on Ok; each pointer
/// passed to `sink` is an owned reference the sink must take over exactly once.
unsafe fn fill_indexed<F, S>(
    rows: usize,
    validity: Option<&Bitmap>,
    mut make: F,
    sink: &mut S,
) -> PyResult<()>
where
    F: FnMut(usize) -> PyResult<*mut ffi::PyObject>,
    S: FnMut(usize, *mut ffi::PyObject),
{
    match validity {
        None => {
            for i in 0..rows {
                let item = make(i)?;
                sink(i, item);
            }
        }
        Some(bm) => {
            for i in 0..rows {
                let item = if bm.is_valid(i) {
                    make(i)?
                } else {
                    none_owned_ptr()
                };
                sink(i, item);
            }
        }
    }
    Ok(())
}

/// Materialize the first `rows` cells of a fixed-width column into `sink`,
/// dispatching the Column variant once and iterating the values buffer
/// directly, with any per-column ctx lookups hoisted out of the loop. Returns
/// Ok(false), touching nothing, for a variant with no fast path (strings,
/// temporal, enum, LowCardinality, ...); those stay on the per-cell route.
///
/// # Safety
///
/// Requires the GIL. Each pointer passed to `sink` is an owned reference the
/// sink must take over exactly once.
pub(super) unsafe fn fill_fixed_width<S>(
    py: Python<'_>,
    col: &Column,
    ctx: &ColumnCtx<'_>,
    rows: usize,
    sink: &mut S,
) -> PyResult<bool>
where
    S: FnMut(usize, *mut ffi::PyObject),
{
    // Safety, for the constructor closures below: pure CPython constructors
    // called with the GIL held; fill_prim null-checks every result.
    match col {
        Column::Int8(c) => fill_prim(
            py,
            &c.values[..rows],
            c.validity.as_ref(),
            |v| unsafe { ffi::PyLong_FromLong(c_long::from(v)) },
            sink,
        )?,
        Column::Int16(c) => fill_prim(
            py,
            &c.values[..rows],
            c.validity.as_ref(),
            |v| unsafe { ffi::PyLong_FromLong(c_long::from(v)) },
            sink,
        )?,
        Column::Int32(c) => fill_prim(
            py,
            &c.values[..rows],
            c.validity.as_ref(),
            |v| unsafe { ffi::PyLong_FromLong(c_long::from(v)) },
            sink,
        )?,
        Column::Int64(c) => fill_prim(
            py,
            &c.values[..rows],
            c.validity.as_ref(),
            |v| unsafe { ffi::PyLong_FromLongLong(v) },
            sink,
        )?,
        Column::UInt8(c) => fill_prim(
            py,
            &c.values[..rows],
            c.validity.as_ref(),
            |v| unsafe { ffi::PyLong_FromLong(c_long::from(v)) },
            sink,
        )?,
        Column::UInt16(c) => fill_prim(
            py,
            &c.values[..rows],
            c.validity.as_ref(),
            |v| unsafe { ffi::PyLong_FromLong(c_long::from(v)) },
            sink,
        )?,
        Column::UInt32(c) => fill_prim(
            py,
            &c.values[..rows],
            c.validity.as_ref(),
            |v| unsafe { ffi::PyLong_FromUnsignedLongLong(v.into()) },
            sink,
        )?,
        Column::UInt64(c) => fill_prim(
            py,
            &c.values[..rows],
            c.validity.as_ref(),
            |v| unsafe { ffi::PyLong_FromUnsignedLongLong(v) },
            sink,
        )?,
        Column::Int128(c) | Column::Int256(c) => fill_indexed(
            rows,
            c.validity.as_ref(),
            |i| unsafe { wide_int_value_ptr(py, c.value(i), true) },
            sink,
        )?,
        Column::UInt128(c) | Column::UInt256(c) => fill_indexed(
            rows,
            c.validity.as_ref(),
            |i| unsafe { wide_int_value_ptr(py, c.value(i), false) },
            sink,
        )?,
        Column::Float32(c) => fill_prim(
            py,
            &c.values[..rows],
            c.validity.as_ref(),
            |v| unsafe { ffi::PyFloat_FromDouble(v.into()) },
            sink,
        )?,
        Column::Float64(c) => fill_prim(
            py,
            &c.values[..rows],
            c.validity.as_ref(),
            |v| unsafe { ffi::PyFloat_FromDouble(v) },
            sink,
        )?,
        Column::BFloat16(c) => fill_prim(
            py,
            &c.values[..rows],
            c.validity.as_ref(),
            |v| unsafe { ffi::PyFloat_FromDouble(bfloat16_to_f32(v).into()) },
            sink,
        )?,
        Column::Interval(c) => fill_prim(
            py,
            &c.values[..rows],
            c.validity.as_ref(),
            |v| unsafe { ffi::PyLong_FromLongLong(v) },
            sink,
        )?,
        Column::Time(c) => {
            if ctx.raw_time_ticks {
                fill_prim(
                    py,
                    &c.values[..rows],
                    c.validity.as_ref(),
                    |v| unsafe { ffi::PyLong_FromLong(c_long::from(v)) },
                    sink,
                )?
            } else {
                fill_indexed(
                    rows,
                    c.validity.as_ref(),
                    |i| Ok(PyDelta::new(py, 0, c.values[i], 0, true)?.into_ptr()),
                    sink,
                )?
            }
        }
        Column::Time64(c) => {
            if ctx.raw_time_ticks {
                fill_prim(
                    py,
                    &c.values[..rows],
                    c.validity.as_ref(),
                    |v| unsafe { ffi::PyLong_FromLongLong(v) },
                    sink,
                )?
            } else {
                let scale = ctx.time_scale;
                fill_indexed(
                    rows,
                    c.validity.as_ref(),
                    |i| Ok(make_time64(py, c.values[i], scale)?.into_ptr()),
                    sink,
                )?
            }
        }
        Column::Bool(c) => match &c.validity {
            None => {
                for i in 0..rows {
                    sink(i, bool_owned_ptr(c.get(i)));
                }
            }
            Some(bm) => {
                for i in 0..rows {
                    let item = if bm.is_valid(i) {
                        bool_owned_ptr(c.get(i))
                    } else {
                        none_owned_ptr()
                    };
                    sink(i, item);
                }
            }
        },
        Column::Uuid(c) => {
            let uctx = ctx.uuid.as_ref().ok_or_else(|| ctx_missing("UUID"))?;
            fill_indexed(
                rows,
                c.validity.as_ref(),
                |i| uuid_value_ptr(py, uctx, c.value(i)),
                sink,
            )?
        }
        Column::Ipv4(c) => {
            let ictx = ctx.ip.as_ref().ok_or_else(|| ctx_missing("IPv4"))?;
            fill_indexed(
                rows,
                c.validity.as_ref(),
                |i| ipv4_value_ptr(py, ictx, c.values[i]),
                sink,
            )?
        }
        Column::Ipv6(c) => {
            let ictx = ctx.ip.as_ref().ok_or_else(|| ctx_missing("IPv6"))?;
            fill_indexed(
                rows,
                c.validity.as_ref(),
                |i| ipv6_value_ptr(py, ictx, c.value(i)),
                sink,
            )?
        }
        Column::Decimal(c) => {
            let cls = ctx
                .decimal_cls
                .as_ref()
                .ok_or_else(|| ctx_missing("Decimal"))?;
            let mut scratch = DecimalScratch::default();
            fill_indexed(
                rows,
                c.validity.as_ref(),
                |i| decimal_value_ptr(cls, &mut scratch, c, i),
                sink,
            )?
        }
        _ => return Ok(false),
    }
    Ok(true)
}

/// Materialize serialized AggregateFunction states as exact Python bytes.
/// The core recovers state boundaries into one offsets run and one contiguous
/// data buffer; the offsets content is trusted as decoder-constructed, the
/// same model as the Utf8 and FixedBinary arms. Python necessarily copies
/// each state into its bytes object; the Arrow exit remains zero-copy for
/// columnar consumers.
///
/// # Safety
///
/// Requires the GIL. Each pointer passed to `sink` is an owned reference the
/// sink must take over exactly once.
pub(super) unsafe fn fill_aggregate_states<S>(
    py: Python<'_>,
    col: &AggregateStateColumn,
    rows: usize,
    sink: &mut S,
) -> PyResult<()>
where
    S: FnMut(usize, *mut ffi::PyObject),
{
    if rows == 0 {
        return Ok(());
    }
    if col.offsets.len() <= rows {
        return Err(PyValueError::new_err(
            "Malformed payload: invalid AggregateFunction state offsets",
        ));
    }
    let mut start = col.offsets[0] as usize;
    for (row, &end) in col.offsets[1..=rows].iter().enumerate() {
        let end = end as usize;
        sink(row, bytes_owned_ptr(py, &col.data[start..end])?);
        start = end;
    }
    Ok(())
}

/// Materialize a dictionary (LowCardinality) column into `sink`: build each
/// referenced dictionary value once through `column_value_nonnull_ptr`, then
/// emit every cell as an INCREF of its cached object — the python codec's
/// object-reuse policy. Nulls live in the index validity (Arrow dictionary
/// convention); invalid cells emit None without touching the dictionary. The
/// cache fills lazily on first reference by a valid index, so an all-null
/// column over an inner type with no object-exit support still reads as None
/// and unreferenced slots cost nothing.
///
/// # Safety
///
/// Requires the GIL. Each pointer passed to `sink` is an owned reference the
/// sink must take over exactly once.
pub(super) unsafe fn fill_dictionary<'py, S>(
    py: Python<'py>,
    col: &DictionaryColumn,
    ctx: &ColumnCtx<'py>,
    rows: usize,
    sink: &mut S,
) -> PyResult<()>
where
    S: FnMut(usize, *mut ffi::PyObject),
{
    let mut cache: Vec<Option<Py<PyAny>>> = Vec::with_capacity(col.values.len());
    cache.resize_with(col.values.len(), || None);
    let cached_item =
        |cache: &mut Vec<Option<Py<PyAny>>>, index: i32| -> PyResult<*mut ffi::PyObject> {
            let slot = usize::try_from(index).map_err(|_| lc_index_err())?;
            let entry = cache.get_mut(slot).ok_or_else(lc_index_err)?;
            if entry.is_none() {
                // Safety: slot < col.values.len() (checked by get_mut); the
                // returned pointer is a valid owned reference; Py takes it
                // over and the Vec drops every cached entry on any exit path.
                let ptr = unsafe { column_value_nonnull_ptr(py, &col.values, ctx, slot, None)? };
                *entry = Some(unsafe { Py::from_owned_ptr(py, ptr) });
            }
            Ok(entry
                .as_ref()
                .expect("entry filled above")
                .clone_ref(py)
                .into_ptr())
        };
    match &col.validity {
        None => {
            for (i, &index) in col.indices[..rows].iter().enumerate() {
                sink(i, cached_item(&mut cache, index)?);
            }
        }
        Some(bm) => {
            for (i, &index) in col.indices[..rows].iter().enumerate() {
                let item = if bm.is_valid(i) {
                    cached_item(&mut cache, index)?
                } else {
                    none_owned_ptr()
                };
                sink(i, item);
            }
        }
    }
    Ok(())
}
