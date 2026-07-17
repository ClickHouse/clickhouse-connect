use super::*;

/// Build the cell at `index` as an owned pointer, assuming the cell is not
/// null; callers check validity first. `cache` is the Array element chain's
/// per-fill cache, if the caller materializes one.
///
/// # Safety
///
/// Returns an owned reference; the caller must take over the reference count.
pub(super) unsafe fn column_value_nonnull_ptr<'py>(
    py: Python<'py>,
    col: &Column,
    ctx: &ColumnCtx<'py>,
    index: usize,
    mut cache: Option<&mut ChainCache<'py>>,
) -> PyResult<*mut ffi::PyObject> {
    match col {
        Column::Bool(c) => ptr_to_result(py, ffi::PyBool_FromLong(c.get(index).into())),
        Column::Int8(c) => ptr_to_result(py, ffi::PyLong_FromLongLong(c.values[index].into())),
        Column::Int16(c) => ptr_to_result(py, ffi::PyLong_FromLongLong(c.values[index].into())),
        Column::Int32(c) => ptr_to_result(py, ffi::PyLong_FromLongLong(c.values[index].into())),
        Column::Int64(c) => ptr_to_result(py, ffi::PyLong_FromLongLong(c.values[index])),
        Column::UInt8(c) => {
            ptr_to_result(py, ffi::PyLong_FromUnsignedLongLong(c.values[index].into()))
        }
        Column::UInt16(c) => {
            ptr_to_result(py, ffi::PyLong_FromUnsignedLongLong(c.values[index].into()))
        }
        Column::UInt32(c) => {
            ptr_to_result(py, ffi::PyLong_FromUnsignedLongLong(c.values[index].into()))
        }
        Column::UInt64(c) => ptr_to_result(py, ffi::PyLong_FromUnsignedLongLong(c.values[index])),
        Column::Int128(c) | Column::Int256(c) => wide_int_value_ptr(py, c.value(index), true),
        Column::UInt128(c) | Column::UInt256(c) => wide_int_value_ptr(py, c.value(index), false),
        Column::Float32(c) => ptr_to_result(py, ffi::PyFloat_FromDouble(c.values[index].into())),
        Column::Float64(c) => ptr_to_result(py, ffi::PyFloat_FromDouble(c.values[index])),
        Column::BFloat16(c) => ptr_to_result(
            py,
            ffi::PyFloat_FromDouble(bfloat16_to_f32(c.values[index]).into()),
        ),
        Column::QBit(_) => Err(PyNotImplementedError::new_err(
            "QBit Python materialization is not implemented",
        )),
        Column::AggregateState(c) => bytes_owned_ptr(py, c.value(index)),
        // The bulk fill above serves top-level and Tuple/Map column runs.
        // This per-cell arm is needed for Array(Nothing) and other recursive
        // container paths.
        Column::Nothing(_) => Ok(none_owned_ptr()),
        Column::Date(c) => Ok(make_date(py, c.values[index] as i64)?.into_ptr()),
        Column::Date32(c) => Ok(make_date(py, c.values[index] as i64)?.into_ptr()),
        Column::DateTime(c) => Ok(make_datetime(py, c.values[index] as i64, 0, ctx)?.into_ptr()),
        Column::DateTime64(c) => {
            let (secs, micros) = dt64_secs_micros(c.values[index], ctx.precision);
            Ok(make_datetime(py, secs, micros, ctx)?.into_ptr())
        }
        Column::Time(c) => {
            if ctx.raw_time_ticks {
                ptr_to_result(py, ffi::PyLong_FromLong(c_long::from(c.values[index])))
            } else {
                Ok(PyDelta::new(py, 0, c.values[index], 0, true)?.into_ptr())
            }
        }
        Column::Time64(c) => {
            if ctx.raw_time_ticks {
                ptr_to_result(py, ffi::PyLong_FromLongLong(c.values[index]))
            } else {
                Ok(make_time64(py, c.values[index], ctx.time_scale)?.into_ptr())
            }
        }
        Column::Interval(c) => ptr_to_result(py, ffi::PyLong_FromLongLong(c.values[index])),
        Column::Utf8(c) => utf8_or_hex_owned_ptr(py, c.value(index)),
        Column::FixedBinary(c) => bytes_owned_ptr(py, c.value(index)),
        // LowCardinality(T): resolve the row's dictionary index, then build the
        // inner value through this same constructor. The cell is known non-null
        // here (callers check the index validity first), so the resolved slot is
        // a real dictionary entry. The ctx already reflects the inner type, so a
        // LowCardinality temporal column gets the right timezone and precision.
        Column::Dictionary(c) => {
            let slot = c
                .indices
                .get(index)
                .copied()
                .and_then(|i| usize::try_from(i).ok())
                .filter(|&slot| slot < c.values.len())
                .ok_or_else(lc_index_err)?;
            match cache {
                // Array element path: build each referenced slot once per
                // chunk and emit clone_ref of the cached object.
                Some(ChainCache::Dict(slots)) => {
                    let entry = slots.get_mut(slot).ok_or_else(lc_index_err)?;
                    if entry.is_none() {
                        let ptr = column_value_nonnull_ptr(py, &c.values, ctx, slot, None)?;
                        *entry = Some(Py::from_owned_ptr(py, ptr));
                    }
                    Ok(entry
                        .as_ref()
                        .expect("entry filled above")
                        .clone_ref(py)
                        .into_ptr())
                }
                _ => column_value_nonnull_ptr(py, &c.values, ctx, slot, None),
            }
        }
        // Enum8/Enum16 carry only the physical signed int; map it to its label
        // string through the per-column value->name map. A value with no defined
        // label becomes None, matching clickhouse-connect's int_map.get default.
        Column::Enum8(c) => enum_value_ptr(ctx, c.values[index] as i64),
        Column::Enum16(c) => enum_value_ptr(ctx, c.values[index] as i64),
        Column::Uuid(c) => {
            let uctx = ctx.uuid.as_ref().ok_or_else(|| ctx_missing("UUID"))?;
            uuid_value_ptr(py, uctx, c.value(index))
        }
        Column::Ipv4(c) => {
            let ictx = ctx.ip.as_ref().ok_or_else(|| ctx_missing("IPv4"))?;
            ipv4_value_ptr(py, ictx, c.values[index])
        }
        Column::Ipv6(c) => {
            let ictx = ctx.ip.as_ref().ok_or_else(|| ctx_missing("IPv6"))?;
            ipv6_value_ptr(py, ictx, c.value(index))
        }
        Column::Decimal(c) => {
            let cls = ctx
                .decimal_cls
                .as_ref()
                .ok_or_else(|| ctx_missing("Decimal"))?;
            let mut scratch = DecimalScratch::default();
            decimal_value_ptr(cls, &mut scratch, c, index)
        }
        // Array(T): materialize row `index` as a Python list of its elements.
        // The offsets buffer is public and could be hand-built, so guard every
        // access: reject a negative offset, out-of-order pair, or an end past
        // the element buffer rather than index out of bounds or panic. Element
        // nulls are handled by column_value_to_owned_ptr, so Array(Nullable(T))
        // yields None elements correctly.
        Column::Array(c) => {
            let ectx = ctx.element.as_deref().ok_or_else(|| ctx_missing("Array"))?;
            let start = c
                .offsets
                .get(index)
                .copied()
                .and_then(|o| usize::try_from(o).ok())
                .ok_or_else(array_bounds_err)?;
            let end = c
                .offsets
                .get(index + 1)
                .copied()
                .and_then(|o| usize::try_from(o).ok())
                .ok_or_else(array_bounds_err)?;
            if start > end || end > c.values.len() {
                return Err(array_bounds_err());
            }
            // Geo point rows: build the list straight from the two flat
            // Float64 runs, skipping the per-element dispatch below.
            if let Some((xs, ys)) = point_pair_slices(&c.values, ectx) {
                if end <= xs.len().min(ys.len()) {
                    return point_list_owned_ptr(py, xs, ys, start, end);
                }
            }
            let count = end - start;
            let list_ptr = ffi::PyList_New(count as ffi::Py_ssize_t);
            if list_ptr.is_null() {
                return Err(PyErr::fetch(py));
            }
            // Safety: list_ptr came from PyList_New, so it is a list and this is
            // the sole owned reference. Binding it makes the error and panic
            // paths drop the partially-filled list; list_dealloc tolerates the
            // NULL slots not yet filled.
            let list = Bound::from_owned_ptr(py, list_ptr).downcast_into_unchecked::<PyList>();
            for slot in 0..count {
                let item = column_value_to_owned_ptr(
                    py,
                    &c.values,
                    ectx,
                    start + slot,
                    cache.as_deref_mut(),
                )?;
                // Safety: slot < count, the list's allocated length, and the
                // list takes over the owned item.
                ffi::PyList_SET_ITEM(list.as_ptr(), slot as ffi::Py_ssize_t, item);
            }
            Ok(list.into_ptr())
        }
        // Tuple(T1, ...): an unnamed tuple materializes as a Python `tuple`, a
        // named tuple as a `dict` keyed by the element names, matching
        // clickhouse-connect's default read format. Field values recurse
        // through column_value_to_owned_ptr, so a Nullable/LowCardinality/nested
        // container element composes and a Nullable element yields None.
        Column::Tuple(c) => {
            let fctx = ctx.fields.as_deref().ok_or_else(|| ctx_missing("Tuple"))?;
            if fctx.len() != c.fields.len() {
                return Err(ctx_count_mismatch("Tuple"));
            }
            match &ctx.tuple_names {
                Some(names) => {
                    let dict_ptr = ffi::PyDict_New();
                    if dict_ptr.is_null() {
                        return Err(PyErr::fetch(py));
                    }
                    // Safety: dict_ptr came from PyDict_New; binding it drops the
                    // partially-filled dict on the error path.
                    let dict =
                        Bound::from_owned_ptr(py, dict_ptr).downcast_into_unchecked::<PyDict>();
                    for (field_idx, field_col) in c.fields.iter().enumerate() {
                        let item = column_value_to_owned_ptr(
                            py,
                            field_col,
                            &fctx[field_idx],
                            index,
                            None,
                        )?;
                        // Take ownership so an error before/at insertion drops it;
                        // PyDict_SetItem does not steal, it increfs the value.
                        let value = Bound::from_owned_ptr(py, item);
                        if ffi::PyDict_SetItem(
                            dict.as_ptr(),
                            names[field_idx].as_ptr(),
                            value.as_ptr(),
                        ) < 0
                        {
                            return Err(PyErr::fetch(py));
                        }
                    }
                    Ok(dict.into_ptr())
                }
                None => {
                    let tuple_ptr = ffi::PyTuple_New(c.fields.len() as ffi::Py_ssize_t);
                    if tuple_ptr.is_null() {
                        return Err(PyErr::fetch(py));
                    }
                    // Safety: tuple_ptr came from PyTuple_New; binding it drops the
                    // partially-filled tuple on the error path (tuple_dealloc
                    // Py_XDECREFs each slot, tolerating the NULL slots).
                    let tuple =
                        Bound::from_owned_ptr(py, tuple_ptr).downcast_into_unchecked::<PyTuple>();
                    for (field_idx, field_col) in c.fields.iter().enumerate() {
                        let item = column_value_to_owned_ptr(
                            py,
                            field_col,
                            &fctx[field_idx],
                            index,
                            None,
                        )?;
                        // Safety: field_idx < tuple len, and the tuple takes over
                        // the owned item.
                        ffi::PyTuple_SET_ITEM(tuple.as_ptr(), field_idx as ffi::Py_ssize_t, item);
                    }
                    Ok(tuple.into_ptr())
                }
            }
        }
        // Map(K, V): materialize row `index` as a Python `dict`. The entries are
        // the flattened Tuple(keys, values) column sliced by the Array-shaped
        // offsets; guard every offset access like the Array arm. Keys are
        // inserted in wire order, so a duplicate key keeps its first position and
        // last value, matching clickhouse-connect's dict(zip(keys, values)).
        Column::Map(c) => {
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
            let start = c
                .offsets
                .get(index)
                .copied()
                .and_then(|o| usize::try_from(o).ok())
                .ok_or_else(map_bounds_err)?;
            let end = c
                .offsets
                .get(index + 1)
                .copied()
                .and_then(|o| usize::try_from(o).ok())
                .ok_or_else(map_bounds_err)?;
            // Bound against the buffers actually indexed below (the decoder
            // guarantees both equal entries.len(), but guard the exact slices
            // like the Array arm rather than the declared tuple length).
            if start > end || end > keys_col.len().min(values_col.len()) {
                return Err(map_bounds_err());
            }
            let dict_ptr = ffi::PyDict_New();
            if dict_ptr.is_null() {
                return Err(PyErr::fetch(py));
            }
            // Safety: dict_ptr came from PyDict_New; binding it drops the
            // partially-filled dict on the error path.
            let dict = Bound::from_owned_ptr(py, dict_ptr).downcast_into_unchecked::<PyDict>();
            for slot in start..end {
                let key = column_value_to_owned_ptr(py, keys_col, &fctx[0], slot, None)?;
                let key = Bound::from_owned_ptr(py, key);
                let value = column_value_to_owned_ptr(py, values_col, &fctx[1], slot, None)?;
                let value = Bound::from_owned_ptr(py, value);
                // PyDict_SetItem increfs both; the Bounds drop our refs after.
                if ffi::PyDict_SetItem(dict.as_ptr(), key.as_ptr(), value.as_ptr()) < 0 {
                    return Err(PyErr::fetch(py));
                }
            }
            Ok(dict.into_ptr())
        }
        // Variant's row routing selects one value from a dense alternative
        // column. This per-cell path is used when Variant is itself inside an
        // Array or another container whose elements are materialized by index;
        // top-level and Tuple/Map Variant runs use the bulk scatter above.
        Column::Variant(c) => {
            let contexts = ctx
                .fields
                .as_deref()
                .ok_or_else(|| ctx_missing("Variant"))?;
            if contexts.len() != c.variants.len() {
                return Err(ctx_count_mismatch("Variant"));
            }
            let (discriminator, offset) = c.value_position(index).ok_or_else(variant_shape_err)?;
            if discriminator == u8::MAX {
                return Ok(none_owned_ptr());
            }
            let alternative = usize::from(discriminator);
            let child = c.variants.get(alternative).ok_or_else(variant_shape_err)?;
            let child_ctx = contexts.get(alternative).ok_or_else(variant_shape_err)?;
            column_value_to_owned_ptr(
                py,
                child,
                child_ctx,
                usize::try_from(offset).map_err(|_| variant_shape_err())?,
                None,
            )
        }
        // Dynamic's per-cell path is used inside Array and other containers
        // whose elements are already being materialized by index. Top-level
        // and Tuple/Map Dynamic runs use the child-major bulk scatter above.
        Column::Dynamic(c) => dynamic_value_owned_ptr(py, c, index, cache),
        Column::Json(c) => json_value_owned_ptr(py, c, ctx, index, cache),
    }
}

/// Convert one little-endian fixed-width integer into an exact Python int.
/// The core intentionally keeps wide integers in their Native/Arrow byte
/// representation, so signedness is supplied by the distinct Column variant.
///
/// # Safety
///
/// Requires the GIL. Returns an owned reference; the caller must take over
/// the reference count.
pub(super) unsafe fn wide_int_value_ptr(
    py: Python<'_>,
    bytes: &[u8],
    signed: bool,
) -> PyResult<*mut ffi::PyObject> {
    // Word-scan fast path: high words all zero (or all ones for a negative
    // signed value) means the value fits one C long long constructor.
    if bytes.len().is_multiple_of(8) {
        if let Some((lo_bytes, high)) = bytes.split_first_chunk::<8>() {
            let lo = u64::from_le_bytes(*lo_bytes);
            let mut hi_or = 0u64;
            let mut hi_and = u64::MAX;
            for word in high.chunks_exact(8) {
                let word = u64::from_le_bytes(word.try_into().expect("chunks_exact(8)"));
                hi_or |= word;
                hi_and &= word;
            }
            if signed {
                let lo = lo as i64;
                if (hi_or == 0 && lo >= 0) || (hi_and == u64::MAX && lo < 0) {
                    return ptr_to_result(py, ffi::PyLong_FromLongLong(lo));
                }
            } else if hi_or == 0 {
                return ptr_to_result(py, ffi::PyLong_FromUnsignedLongLong(lo));
            }
        }
    }
    #[cfg(not(Py_3_13))]
    {
        ptr_to_result(
            py,
            ffi::_PyLong_FromByteArray(bytes.as_ptr(), bytes.len(), 1, c_int::from(signed)),
        )
    }
    #[cfg(Py_3_13)]
    {
        let ptr = if signed {
            ffi::PyLong_FromNativeBytes(
                bytes.as_ptr().cast(),
                bytes.len(),
                ffi::Py_ASNATIVEBYTES_LITTLE_ENDIAN,
            )
        } else {
            ffi::PyLong_FromUnsignedNativeBytes(
                bytes.as_ptr().cast(),
                bytes.len(),
                ffi::Py_ASNATIVEBYTES_LITTLE_ENDIAN,
            )
        };
        ptr_to_result(py, ptr)
    }
}

/// Build a `uuid.UUID` from the 16 raw wire bytes: `UUID.__new__(UUID)`, then
/// `object.__setattr__` of `int` and `is_safe` (SafeUUID.unsafe), matching the
/// Cython codec's read_uuid_col. The wire int is le(b[0..8]) << 64 | le(b[8..16]),
/// which is `from_le_bytes` with the halves swapped.
pub(super) fn uuid_value_ptr(
    py: Python<'_>,
    ctx: &UuidCtx<'_>,
    bytes: &[u8],
) -> PyResult<*mut ffi::PyObject> {
    let b: &[u8; 16] = bytes.try_into().map_err(|_| fixed_width_err("UUID"))?;
    let int_val = u128::from_le_bytes(*b).rotate_left(64);
    let value = ctx.new.call1((&ctx.cls,))?;
    ctx.object_setattr
        .call1((&value, intern!(py, "int"), int_val))?;
    ctx.object_setattr
        .call1((&value, intern!(py, "is_safe"), &ctx.unsafe_marker))?;
    Ok(value.into_ptr())
}

/// Build an `ipaddress.IPv4Address` from the numeric address value.
pub(super) fn ipv4_value_ptr(
    py: Python<'_>,
    ctx: &IpCtx<'_>,
    value: u32,
) -> PyResult<*mut ffi::PyObject> {
    let addr = ctx.new.call1((&ctx.cls,))?;
    addr.setattr(intern!(py, "_ip"), value)?;
    Ok(addr.into_ptr())
}

/// Build an `ipaddress.IPv6Address` from the 16 network-order wire bytes,
/// always IPv6Address even for a v4-mapped value, matching _read_binary_ip.
pub(super) fn ipv6_value_ptr(
    py: Python<'_>,
    ctx: &IpCtx<'_>,
    bytes: &[u8],
) -> PyResult<*mut ffi::PyObject> {
    let b: &[u8; 16] = bytes.try_into().map_err(|_| fixed_width_err("IPv6"))?;
    let int_val = u128::from_be_bytes(*b);
    let addr = ctx.new.call1((&ctx.cls,))?;
    addr.setattr(intern!(py, "_ip"), int_val)?;
    if ctx.set_scope_id {
        addr.setattr(intern!(py, "_scope_id"), py.None())?;
    }
    Ok(addr.into_ptr())
}

/// Reusable buffers for Decimal text rendering: the magnitude digits and the
/// composed constructor argument.
#[derive(Default)]
pub(super) struct DecimalScratch {
    digits: String,
    text: String,
}

/// Build a `decimal.Decimal` for the cell: render the unscaled value as exact
/// decimal text (sign, integer digits, exactly `scale` fractional digits) and
/// call the class once. The text form yields the same value and exponent as
/// the python codec's `Decimal(unscaled).scaleb(-scale)`.
pub(super) fn decimal_value_ptr(
    cls: &Bound<'_, PyAny>,
    scratch: &mut DecimalScratch,
    col: &DecimalColumn,
    index: usize,
) -> PyResult<*mut ffi::PyObject> {
    scratch.digits.clear();
    let negative = write_decimal_magnitude(col.value(index), &mut scratch.digits)?;
    compose_decimal_text(
        &mut scratch.text,
        negative,
        &scratch.digits,
        col.scale as usize,
    );
    Ok(cls.call1((scratch.text.as_str(),))?.into_ptr())
}

/// Write the magnitude digits (no sign, no leading zeros, "0" for zero) of a
/// little-endian two's-complement integer of width 4/8/16/32 bytes into `out`;
/// returns whether the value is negative.
fn write_decimal_magnitude(bytes: &[u8], out: &mut String) -> PyResult<bool> {
    use std::fmt::Write as _;
    match bytes.len() {
        4 => {
            let v = i32::from_le_bytes(bytes.try_into().expect("width checked"));
            let _ = write!(out, "{}", v.unsigned_abs());
            Ok(v < 0)
        }
        8 => {
            let v = i64::from_le_bytes(bytes.try_into().expect("width checked"));
            let _ = write!(out, "{}", v.unsigned_abs());
            Ok(v < 0)
        }
        16 => {
            let v = i128::from_le_bytes(bytes.try_into().expect("width checked"));
            let _ = write!(out, "{}", v.unsigned_abs());
            Ok(v < 0)
        }
        32 => {
            let mut limbs = [0u64; 4];
            for (limb, chunk) in limbs.iter_mut().zip(bytes.chunks_exact(8)) {
                *limb = u64::from_le_bytes(chunk.try_into().expect("chunks_exact(8)"));
            }
            let negative = limbs[3] >> 63 == 1;
            if negative {
                negate_limbs(&mut limbs);
            }
            write_u256_digits(limbs, out);
            Ok(negative)
        }
        w => Err(PyValueError::new_err(format!(
            "Malformed payload: unsupported Decimal width {w}"
        ))),
    }
}

/// Two's-complement negate a 256-bit little-endian limb array in place.
fn negate_limbs(limbs: &mut [u64; 4]) {
    let mut carry = 1u64;
    for limb in limbs.iter_mut() {
        let (v, overflowed) = (!*limb).overflowing_add(carry);
        *limb = v;
        carry = u64::from(overflowed);
    }
}

/// Divide a 256-bit little-endian limb magnitude in place by `divisor`,
/// returning the remainder. Standard long division, most-significant limb first.
fn div_rem_limbs(limbs: &mut [u64; 4], divisor: u64) -> u64 {
    let mut rem: u128 = 0;
    for limb in limbs.iter_mut().rev() {
        let cur = (rem << 64) | u128::from(*limb);
        *limb = (cur / u128::from(divisor)) as u64;
        rem = cur % u128::from(divisor);
    }
    rem as u64
}

/// Write the decimal digits of a 256-bit little-endian limb magnitude: repeated
/// divmod by 1e19 yields base-1e19 chunks, most significant unpadded, the rest
/// zero-padded to 19 digits. At most 5 chunks (2^255 has 77 digits).
fn write_u256_digits(mut limbs: [u64; 4], out: &mut String) {
    use std::fmt::Write as _;
    const CHUNK: u64 = 10_000_000_000_000_000_000; // 1e19
    let mut chunks = [0u64; 5];
    let mut count = 0;
    loop {
        chunks[count] = div_rem_limbs(&mut limbs, CHUNK);
        count += 1;
        if limbs == [0u64; 4] {
            break;
        }
    }
    let _ = write!(out, "{}", chunks[count - 1]);
    for &chunk in chunks[..count - 1].iter().rev() {
        let _ = write!(out, "{chunk:019}");
    }
}

/// Compose the Decimal constructor text: optional '-', integer digits, and for
/// scale > 0 a '.' with exactly `scale` fractional digits. `digits` is the
/// magnitude with no sign or leading zeros ("0" only for zero).
fn compose_decimal_text(out: &mut String, negative: bool, digits: &str, scale: usize) {
    out.clear();
    if negative {
        out.push('-');
    }
    if scale == 0 {
        out.push_str(digits);
        return;
    }
    if digits.len() > scale {
        let split = digits.len() - scale;
        out.push_str(&digits[..split]);
        out.push('.');
        out.push_str(&digits[split..]);
    } else {
        out.push_str("0.");
        for _ in 0..(scale - digits.len()) {
            out.push('0');
        }
        out.push_str(digits);
    }
}

/// Map an enum's physical integer to its label string, or None for a value with
/// no defined label (matching clickhouse-connect's `int_map.get(value, None)`).
///
/// # Safety
///
/// Returns an owned reference; the caller must take over the reference count.
unsafe fn enum_value_ptr(ctx: &ColumnCtx<'_>, value: i64) -> PyResult<*mut ffi::PyObject> {
    match ctx.enum_names.as_ref().and_then(|m| m.get(&value)) {
        Some(name) => Ok(name.clone().into_ptr()),
        None => Ok(none_owned_ptr()),
    }
}
