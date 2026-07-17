use super::*;

/// Materialize a Variant column child by child: route each logical row to its
/// dense child slot from the discriminator run, then scatter each child's bulk
/// fill into those row positions.
///
/// # Safety
///
/// Requires the GIL. `fill_column`'s sink contract applies.
pub(super) unsafe fn fill_variant<'py, S>(
    py: Python<'py>,
    col: &VariantColumn,
    ctx: &ColumnCtx<'py>,
    rows: usize,
    sink: &mut S,
) -> PyResult<()>
where
    S: FnMut(usize, *mut ffi::PyObject),
{
    let contexts = ctx
        .fields
        .as_deref()
        .ok_or_else(|| ctx_missing("Variant"))?;
    if contexts.len() != col.variants.len() {
        return Err(ctx_count_mismatch("Variant"));
    }
    let discriminators = col.discriminators();
    if rows != col.len() || discriminators.len() != rows {
        return Err(variant_shape_err());
    }

    let mut starts = Vec::with_capacity(col.variants.len() + 1);
    starts.push(0usize);
    for child in &col.variants {
        let next = starts
            .last()
            .copied()
            .unwrap_or_default()
            .checked_add(child.len())
            .ok_or_else(variant_shape_err)?;
        starts.push(next);
    }
    // Occurrence ordinals are the dense child offsets, so per-alternative
    // running counters assign each non-NULL row a distinct slot; the written
    // counter is the completeness check.
    let mut destinations = vec![0usize; starts.last().copied().unwrap_or_default()];
    let mut counters = vec![0usize; col.variants.len()];
    let mut written = 0usize;
    let mut nulls = 0usize;
    for (row, &discriminator) in discriminators.iter().enumerate() {
        if discriminator == u8::MAX {
            nulls += 1;
            sink(row, none_owned_ptr());
            continue;
        }
        let alternative = usize::from(discriminator);
        if alternative >= col.variants.len() {
            return Err(variant_shape_err());
        }
        let slot = starts[alternative] + counters[alternative];
        if slot >= starts[alternative + 1] {
            return Err(variant_shape_err());
        }
        counters[alternative] += 1;
        destinations[slot] = row;
        written += 1;
    }
    if nulls != col.nulls.len || written != destinations.len() {
        return Err(variant_shape_err());
    }

    for (alternative, (child, child_ctx)) in col.variants.iter().zip(contexts).enumerate() {
        let positions = &destinations[starts[alternative]..starts[alternative + 1]];
        let mut scatter = |child_row: usize, item: *mut ffi::PyObject| {
            sink(positions[child_row], item);
        };
        let mut erased: DynSink<'_> = &mut scatter;
        fill_column(py, child, child_ctx, child.len(), &mut erased)?;
    }
    Ok(())
}

/// Materialize a Dynamic column child by child. Dynamic carries block-local
/// child ids and occurrence-ordinal offsets rather than Variant's raw UInt8
/// discriminator run, but both use the same dense child-major layout.
/// SharedVariant cells decode to typed Python values through the core's
/// single-value binary decoder and the same per-type conversion machinery as
/// typed children; AggregateFunction cells and unsupported descriptors keep
/// the raw cell bytes. The Arrow C Stream export keeps every SharedVariant
/// cell as bytes for schema stability; only these object exits decode them.
///
/// # Safety
///
/// Requires the GIL. `fill_column`'s sink contract applies.
pub(super) unsafe fn fill_dynamic<'py, S>(
    py: Python<'py>,
    col: &DynamicColumn,
    rows: usize,
    sink: &mut S,
) -> PyResult<()>
where
    S: FnMut(usize, *mut ffi::PyObject),
{
    if rows != col.len() || col.offsets.len() != rows {
        return Err(dynamic_shape_err());
    }

    let mut starts = Vec::with_capacity(col.children.len() + 1);
    starts.push(0usize);
    for child in &col.children {
        let next = starts
            .last()
            .copied()
            .unwrap_or_default()
            .checked_add(child.len())
            .ok_or_else(dynamic_shape_err)?;
        starts.push(next);
    }
    let mut destinations = vec![0usize; starts.last().copied().unwrap_or_default()];
    let mut counters = vec![0usize; col.children.len()];
    let mut nulls = 0usize;
    for (row, (&type_id, &offset)) in col.type_ids.iter().zip(&col.offsets).enumerate() {
        let offset = usize::try_from(offset).map_err(|_| dynamic_shape_err())?;
        if type_id == u32::MAX {
            if offset != nulls {
                return Err(dynamic_shape_err());
            }
            nulls += 1;
            sink(row, none_owned_ptr());
            continue;
        }
        let child = usize::try_from(type_id).map_err(|_| dynamic_shape_err())?;
        let counter = counters.get_mut(child).ok_or_else(dynamic_shape_err)?;
        if offset != *counter || offset >= col.children[child].len() {
            return Err(dynamic_shape_err());
        }
        destinations[starts[child] + offset] = row;
        *counter += 1;
    }
    if nulls != col.nulls.len
        || counters
            .iter()
            .zip(&col.children)
            .any(|(&count, child)| count != child.len())
    {
        return Err(dynamic_shape_err());
    }

    for (child_index, child) in col.children.iter().enumerate() {
        let positions = &destinations[starts[child_index]..starts[child_index + 1]];
        match child {
            DynamicChild::Typed { ch_type, values } => {
                // Dynamic cells are type-erased at the driver boundary. As for
                // Variant alternatives, finalize temporal leaves here instead
                // of exposing raw ticks to a walker that cannot see the child
                // type.
                let child_ctx = prepare_column_ctx(py, ch_type, false)?;
                let mut scatter = |child_row: usize, item: *mut ffi::PyObject| {
                    sink(positions[child_row], item);
                };
                let mut erased: DynSink<'_> = &mut scatter;
                fill_column(py, values, &child_ctx, child.len(), &mut erased)?;
            }
            DynamicChild::Shared(values) => {
                // Cells with the same descriptor share one prepared context
                // per fill.
                let mut ctx_cache: SharedCtxCache<'py> = Vec::new();
                for (child_row, &position) in positions.iter().enumerate() {
                    let item =
                        shared_cell_owned_ptr(py, values.value(child_row), Some(&mut ctx_cache))?;
                    sink(position, item);
                }
            }
        }
    }
    Ok(())
}

/// Materialize one logical Dynamic cell. Used by recursive container/JSON
/// exits; top-level Dynamic columns keep the child-major bulk fill above.
pub(super) unsafe fn dynamic_value_owned_ptr<'py>(
    py: Python<'py>,
    col: &DynamicColumn,
    index: usize,
    mut cache: Option<&mut ChainCache<'py>>,
) -> PyResult<*mut ffi::PyObject> {
    let (&type_id, &offset) = col
        .type_ids
        .get(index)
        .zip(col.offsets.get(index))
        .ok_or_else(dynamic_shape_err)?;
    let offset = usize::try_from(offset).map_err(|_| dynamic_shape_err())?;
    if type_id == u32::MAX {
        if offset >= col.nulls.len {
            return Err(dynamic_shape_err());
        }
        return Ok(none_owned_ptr());
    }
    let child_index = usize::try_from(type_id).map_err(|_| dynamic_shape_err())?;
    let child = col
        .children
        .get(child_index)
        .ok_or_else(dynamic_shape_err)?;
    if offset >= child.len() {
        return Err(dynamic_shape_err());
    }
    match child {
        DynamicChild::Typed { ch_type, values } => {
            if let Some(ChainCache::Dynamic { contexts, .. }) = cache.as_deref_mut() {
                if let Some(entry) = contexts.get_mut(child_index) {
                    if entry.is_none() {
                        *entry = Some(prepare_column_ctx(py, ch_type, false)?);
                    }
                    let child_ctx = entry.as_ref().expect("entry filled above");
                    return column_value_to_owned_ptr(py, values, child_ctx, offset, None);
                }
            }
            let child_ctx = prepare_column_ctx(py, ch_type, false)?;
            column_value_to_owned_ptr(py, values, &child_ctx, offset, None)
        }
        DynamicChild::Shared(values) => {
            let shared_cache = match cache {
                Some(ChainCache::Dynamic { shared, .. }) => Some(shared),
                _ => None,
            };
            shared_cell_owned_ptr(py, values.value(offset), shared_cache)
        }
    }
}

/// Materialize one SharedVariant cell (binary type descriptor + one
/// `serializeBinary` value) as a typed Python object: decode the value to a
/// one-row Column and route it through the same conversion machinery as a
/// typed column. AggregateFunction cells (opaque state payloads) and cells
/// whose descriptor does not parse or whose value encoding is unsupported
/// stay raw Python bytes. A cell whose descriptor parsed but whose payload
/// fails to decode is a ValueError.
///
/// # Safety
///
/// Requires the GIL. Returns an owned reference; the caller must take over
/// the reference count.
pub(super) unsafe fn shared_cell_owned_ptr<'py>(
    py: Python<'py>,
    cell: &[u8],
    mut ctx_cache: Option<&mut SharedCtxCache<'py>>,
) -> PyResult<*mut ffi::PyObject> {
    // Cells are varint-length framed, so the raw bytes are always
    // recoverable; an unknown descriptor tag is a future server type, not
    // corruption. Every descriptor-parse failure falls back to bytes.
    if let Some(cache) = ctx_cache.as_deref_mut() {
        // Binary type descriptors are a prefix code (the parser consumes
        // exactly the descriptor from the front), so a cached descriptor
        // prefix identifies the type and the cursor advance without a parse.
        if let Some((key, ch_type, ctx)) = cache.iter().find(|(key, ..)| cell.starts_with(key)) {
            return shared_payload_owned_ptr(py, ch_type, ctx, cell, key.len());
        }
    }
    let Ok((ch_type, consumed)) = read_binary_type_prefix(cell) else {
        return bytes_owned_ptr(py, cell);
    };
    if matches!(ch_type, ChType::AggregateFunction { .. }) {
        return bytes_owned_ptr(py, cell);
    }
    let payload = &cell[consumed..];
    if let Some(fast) = shared_scalar_owned_ptr(py, &ch_type, payload) {
        // Scalar contexts need no Python machinery; cache for later cells.
        if let Some(cache) = ctx_cache {
            let ctx = prepare_column_ctx(py, &ch_type, false)?;
            cache.push((cell[..consumed].to_vec(), ch_type, ctx));
        }
        return fast;
    }
    // Decode before preparing a context, so a bytes-fallback descriptor
    // (Variant, Dynamic, JSON, ...) never runs the Python ctx machinery,
    // which can raise (for one, a nested unknown timezone).
    let column = match decode_binary_value(&ch_type, payload) {
        Ok(column) => column,
        Err(BinaryValueError::Unsupported(_)) => return bytes_owned_ptr(py, cell),
        Err(err) => return Err(shared_cell_err(&err)),
    };
    let ctx = prepare_column_ctx(py, &ch_type, false)?;
    if let Some(cache) = ctx_cache {
        cache.push((cell[..consumed].to_vec(), ch_type, ctx));
        let (_, _, ctx) = cache.last().expect("pushed above");
        column_value_to_owned_ptr(py, &column, ctx, 0, None)
    } else {
        column_value_to_owned_ptr(py, &column, &ctx, 0, None)
    }
}

/// Materialize the single value after a cache-hit cell's `consumed`-byte
/// descriptor. Cached descriptors already passed the AggregateFunction gate;
/// an Unsupported payload (an `Array(Nothing)` value, say) still falls back
/// to bytes.
///
/// # Safety
///
/// Requires the GIL. Returns an owned reference; the caller must take over
/// the reference count.
unsafe fn shared_payload_owned_ptr<'py>(
    py: Python<'py>,
    ch_type: &ChType,
    ctx: &ColumnCtx<'py>,
    cell: &[u8],
    consumed: usize,
) -> PyResult<*mut ffi::PyObject> {
    let payload = &cell[consumed..];
    if let Some(fast) = shared_scalar_owned_ptr(py, ch_type, payload) {
        return fast;
    }
    let column = match decode_binary_value(ch_type, payload) {
        Ok(column) => column,
        Err(BinaryValueError::Unsupported(_)) => return bytes_owned_ptr(py, cell),
        Err(err) => return Err(shared_cell_err(&err)),
    };
    column_value_to_owned_ptr(py, &column, ctx, 0, None)
}

/// Direct materialization for the hot scalar shared-cell types, skipping the
/// one-row Column build. Matches `decode_binary_value` semantics: truncation
/// and trailing bytes are errors. `None` falls through to the generic route.
///
/// # Safety
///
/// Requires the GIL. Returns an owned reference; the caller must take over
/// the reference count.
unsafe fn shared_scalar_owned_ptr(
    py: Python<'_>,
    ch_type: &ChType,
    payload: &[u8],
) -> Option<PyResult<*mut ffi::PyObject>> {
    unsafe fn fixed<const N: usize>(
        py: Python<'_>,
        payload: &[u8],
        build: impl FnOnce([u8; N]) -> *mut ffi::PyObject,
    ) -> PyResult<*mut ffi::PyObject> {
        match payload.try_into() {
            Ok(bytes) => ptr_to_result(py, build(bytes)),
            // Match decode_binary_value's wording exactly.
            Err(_) => Err(shared_cell_err(&BinaryValueError::Invalid(
                if payload.len() < N {
                    "truncated value payload".to_string()
                } else {
                    format!("{} trailing bytes after the value", payload.len() - N)
                },
            ))),
        }
    }
    Some(match ch_type {
        ChType::String => shared_string_owned_ptr(py, payload),
        ChType::Bool => fixed(py, payload, |[b]: [u8; 1]| {
            ffi::PyBool_FromLong(c_long::from(b != 0))
        }),
        ChType::Int8 => fixed(py, payload, |b: [u8; 1]| {
            ffi::PyLong_FromLongLong(i8::from_le_bytes(b).into())
        }),
        ChType::Int16 => fixed(py, payload, |b: [u8; 2]| {
            ffi::PyLong_FromLongLong(i16::from_le_bytes(b).into())
        }),
        ChType::Int32 => fixed(py, payload, |b: [u8; 4]| {
            ffi::PyLong_FromLongLong(i32::from_le_bytes(b).into())
        }),
        ChType::Int64 => fixed(py, payload, |b: [u8; 8]| {
            ffi::PyLong_FromLongLong(i64::from_le_bytes(b))
        }),
        ChType::UInt8 => fixed(py, payload, |b: [u8; 1]| {
            ffi::PyLong_FromUnsignedLongLong(u8::from_le_bytes(b).into())
        }),
        ChType::UInt16 => fixed(py, payload, |b: [u8; 2]| {
            ffi::PyLong_FromUnsignedLongLong(u16::from_le_bytes(b).into())
        }),
        ChType::UInt32 => fixed(py, payload, |b: [u8; 4]| {
            ffi::PyLong_FromUnsignedLongLong(u32::from_le_bytes(b).into())
        }),
        ChType::UInt64 => fixed(py, payload, |b: [u8; 8]| {
            ffi::PyLong_FromUnsignedLongLong(u64::from_le_bytes(b))
        }),
        ChType::Float32 => fixed(py, payload, |b: [u8; 4]| {
            ffi::PyFloat_FromDouble(f32::from_le_bytes(b).into())
        }),
        ChType::Float64 => fixed(py, payload, |b: [u8; 8]| {
            ffi::PyFloat_FromDouble(f64::from_le_bytes(b))
        }),
        _ => return None,
    })
}

/// String shared-cell fast path: varint length plus raw bytes, materialized
/// with the same invalid-UTF-8 hex fallback as the bulk String fill.
///
/// # Safety
///
/// Requires the GIL. Returns an owned reference; the caller must take over
/// the reference count.
unsafe fn shared_string_owned_ptr(py: Python<'_>, payload: &[u8]) -> PyResult<*mut ffi::PyObject> {
    let mut reader = ByteReader::new(payload);
    let bytes = (|| -> Result<&[u8], BinaryValueError> {
        let len = usize::try_from(reader.read_varint()?)
            .map_err(|_| BinaryValueError::Invalid("String value length overflows usize".into()))?;
        if i32::try_from(len).is_err() {
            return Err(BinaryValueError::Invalid(
                "String value exceeds i32 offset range".into(),
            ));
        }
        let bytes = reader.read_slice(len)?;
        if reader.remaining() != 0 {
            return Err(BinaryValueError::Invalid(format!(
                "{} trailing bytes after the value",
                reader.remaining()
            )));
        }
        Ok(bytes)
    })()
    .map_err(|err| shared_cell_err(&err))?;
    utf8_or_hex_owned_ptr(py, bytes)
}
