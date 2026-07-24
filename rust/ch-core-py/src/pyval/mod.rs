use std::collections::HashMap;
#[cfg(not(Py_3_13))]
use std::ffi::c_int;
use std::ffi::{c_char, c_long};

use pyo3::exceptions::{PyUnicodeDecodeError, PyValueError};
use pyo3::ffi;
use pyo3::intern;
use pyo3::prelude::*;
use pyo3::types::{PyDate, PyDateTime, PyDelta, PyDict, PyList, PyString, PyTuple};

use ch_core_rs::bitmap::Bitmap;
use ch_core_rs::column::{
    AggregateStateColumn, Column, DecimalColumn, DictionaryColumn, DynamicChild, DynamicColumn,
    JsonBody, JsonColumn, MapColumn, QBitColumn, StructuredJson, TupleColumn, VariantColumn,
};
use ch_core_rs::native::binary_value::{
    decode_binary_value, read_binary_type_prefix, BinaryValueError,
};
use ch_core_rs::native::varint::ByteReader;
use ch_core_rs::schema::ChType;

mod containers;
mod ctx;
mod errors;
mod fixed;
mod json;
mod ptr;
mod qbit;
mod scalar;
mod temporal;
mod variant_dynamic;

use containers::{fill_map, fill_tuple, point_list_owned_ptr, point_pair_slices};
pub(crate) use ctx::{prepare_column_ctx, ColumnCtx};
use ctx::{prepare_json_path, IpCtx, JsonPath, UuidCtx};
use errors::*;
use fixed::{
    bfloat16_to_f32, column_validity, fill_aggregate_states, fill_dictionary, fill_fixed_width,
};
use json::{fill_json, json_value_owned_ptr, JsonPathCache};
use ptr::*;
use qbit::{fill_qbit, qbit_value_owned_ptr};
use scalar::{
    column_value_nonnull_ptr, decimal_value_ptr, ipv4_value_ptr, ipv6_value_ptr, uuid_value_ptr,
    wide_int_value_ptr, DecimalScratch,
};
use temporal::{dt64_secs_micros, make_date, make_datetime, make_time64};
use variant_dynamic::{dynamic_value_owned_ptr, fill_dynamic, fill_variant, shared_cell_owned_ptr};

/// Allocate a dict presized for `len` entries via `_PyDict_NewPresized` where
/// pyo3-ffi declares it; otherwise a plain `PyDict_New`, costing only the
/// presize.
///
/// # Safety
///
/// Requires the GIL. Returns an owned reference, or null with an error set.
unsafe fn dict_new_presized(len: ffi::Py_ssize_t) -> *mut ffi::PyObject {
    #[cfg(all(not(Py_LIMITED_API), not(PyPy)))]
    {
        ffi::_PyDict_NewPresized(len)
    }
    #[cfg(any(Py_LIMITED_API, PyPy))]
    {
        let _ = len;
        ffi::PyDict_New()
    }
}

/// Type-erased sink. The Tuple/Map fills recurse through `fill_column` with
/// fresh closure types per nesting level; erasing at the container boundary
/// keeps monomorphization finite while top-level fills stay static.
type DynSink<'a> = &'a mut dyn FnMut(usize, *mut ffi::PyObject);

/// Materialize `rows` cells of `col` into `sink`: bulk fixed-width and
/// dictionary fills first, hoisted Tuple/Map fills next, per-cell fallback
/// last with the validity branch hoisted.
///
/// # Safety
///
/// Requires the GIL. `sink` is called exactly once per row with the cell's
/// positional row index and an owned reference it must take over. Variant and
/// Dynamic fills call it in child-major order; every other fill calls it in
/// ascending row order, which `materialize_run`'s push sink enforces. The sink must keep
/// every item alive until this call returns because Tuple fills write into
/// containers after sinking them.
pub(crate) unsafe fn fill_column<'py, S>(
    py: Python<'py>,
    col: &Column,
    ctx: &ColumnCtx<'py>,
    rows: usize,
    sink: &mut S,
) -> PyResult<()>
where
    S: FnMut(usize, *mut ffi::PyObject),
{
    if matches!(col, Column::Nothing(_)) {
        // Nothing has no host value. Its optional bitmap only preserves the
        // structural null map of Nullable(Nothing) for Native re-encoding;
        // both valid and invalid bits materialize as Python None. Keep this
        // as a column-wide fill so flat results do not check that bitmap or
        // redispatch the Column enum for every row.
        for i in 0..rows {
            sink(i, none_owned_ptr());
        }
        return Ok(());
    }
    if let Column::AggregateState(states) = col {
        return fill_aggregate_states(py, states, rows, sink);
    }
    if let Column::QBit(qbit) = col {
        return fill_qbit(py, qbit, rows, sink);
    }
    if fill_fixed_width(py, col, ctx, rows, sink)? {
        return Ok(());
    }
    match col {
        Column::Dictionary(dict) => fill_dictionary(py, dict, ctx, rows, sink),
        Column::Variant(c) => fill_variant(py, c, ctx, rows, sink),
        Column::Dynamic(c) => fill_dynamic(py, c, rows, sink),
        Column::Json(c) => fill_json(py, c, ctx, rows, sink),
        Column::Tuple(c) => fill_tuple(py, c, ctx, rows, sink),
        Column::Map(c) => fill_map(py, c, ctx, rows, sink),
        _ => {
            let mut chain_cache = new_array_chain_cache(col);
            match column_validity(col) {
                None => {
                    for i in 0..rows {
                        let item = column_value_nonnull_ptr(py, col, ctx, i, chain_cache.as_mut())?;
                        sink(i, item);
                    }
                }
                Some(bm) => {
                    for i in 0..rows {
                        let item = if bm.is_valid(i) {
                            column_value_nonnull_ptr(py, col, ctx, i, chain_cache.as_mut())?
                        } else {
                            none_owned_ptr()
                        };
                        sink(i, item);
                    }
                }
            }
            Ok(())
        }
    }
}

/// Materialize the first `rows` cells of `col` into owned objects through the
/// bulk fill machinery. The indexed slots accept Variant and Dynamic's
/// child-major scatter while preserving logical row order for Map key/value
/// runs. Error paths drop whatever was already produced.
unsafe fn materialize_run<'py>(
    py: Python<'py>,
    col: &Column,
    ctx: &ColumnCtx<'py>,
    rows: usize,
) -> PyResult<Vec<Py<PyAny>>> {
    if !matches!(col, Column::Variant(_) | Column::Dynamic(_)) {
        let mut out: Vec<Py<PyAny>> = Vec::with_capacity(rows);
        // Items sunk out of ascending order are an internal error; they stay
        // alive here until the fill returns, per the sink contract.
        let mut misordered: Vec<Py<PyAny>> = Vec::new();
        {
            let mut sink = |i: usize, item: *mut ffi::PyObject| {
                // Safety: item is an owned reference the sink takes over.
                let item = unsafe { Py::from_owned_ptr(py, item) };
                if i == out.len() {
                    out.push(item);
                } else {
                    misordered.push(item);
                }
            };
            let mut erased: DynSink<'_> = &mut sink;
            fill_column(py, col, ctx, rows, &mut erased)?;
        }
        if !misordered.is_empty() {
            return Err(PyValueError::new_err(
                "internal error: column fill ran out of row order",
            ));
        }
        if out.len() != rows {
            return Err(PyValueError::new_err(
                "internal error: column fill produced the wrong row count",
            ));
        }
        return Ok(out);
    }

    let mut out: Vec<Option<Py<PyAny>>> = Vec::with_capacity(rows);
    out.resize_with(rows, || None);
    {
        let mut sink = |i: usize, item: *mut ffi::PyObject| {
            // Safety: item is an owned reference the sink takes over.
            out[i] = Some(unsafe { Py::from_owned_ptr(py, item) });
        };
        let mut erased: DynSink<'_> = &mut sink;
        fill_column(py, col, ctx, rows, &mut erased)?;
    }
    out.into_iter()
        .map(|item| {
            item.ok_or_else(|| PyValueError::new_err("internal error: column fill omitted a row"))
        })
        .collect()
}

/// Lazy cache of materialized dictionary slot objects, one entry per slot.
type DictSlotCache = Vec<Option<Py<PyAny>>>;

/// Descriptor bytes -> parsed type plus prepared context for SharedVariant
/// and JSON shared-data cells. Distinct descriptors per block are typically
/// single digits, so a linear scan beats hashing.
type SharedCtxCache<'py> = Vec<(Vec<u8>, ChType, ColumnCtx<'py>)>;

/// Per-fill cache for a JSON column inside an Array element chain: the
/// resolved `json.loads`, prepared dynamic/shared paths, shared-cell
/// descriptor contexts, and the once-per-column typed-order check. All fill
/// lazily on first reference.
#[derive(Default)]
struct JsonChainCache<'py> {
    loads: Option<Bound<'py, PyAny>>,
    paths: JsonPathCache<'py>,
    shared: SharedCtxCache<'py>,
    typed_order_checked: bool,
    estimated_keys: Option<ffi::Py_ssize_t>,
}

/// A per-fill cache for the terminal of an Array column's element chain. One
/// cache per array column per chunk, threaded through the per-cell path.
/// `Dict` caches materialized LowCardinality dictionary slots so repeated
/// labels share one object, matching `fill_dictionary`'s reuse policy.
/// `Dynamic` caches one prepared `ColumnCtx` per block-local Dynamic child,
/// plus one per distinct SharedVariant descriptor, so
/// `column_value_nonnull_ptr` does not rebuild contexts per cell. `Json`
/// carries the per-column JSON state. All fill lazily on first reference.
enum ChainCache<'py> {
    Dict(DictSlotCache),
    Dynamic {
        contexts: Vec<Option<ColumnCtx<'py>>>,
        shared: SharedCtxCache<'py>,
    },
    Json(JsonChainCache<'py>),
}

/// Build the cache for the Dictionary or Dynamic column terminating an Array
/// column's element chain, if any.
fn new_array_chain_cache<'py>(col: &Column) -> Option<ChainCache<'py>> {
    fn chain_terminal(col: &Column) -> Option<&Column> {
        match col {
            Column::Array(c) => chain_terminal(&c.values),
            Column::Dictionary(_) | Column::Dynamic(_) | Column::Json(_) => Some(col),
            _ => None,
        }
    }
    fn empty_slots<T>(len: usize) -> Vec<Option<T>> {
        let mut slots = Vec::with_capacity(len);
        slots.resize_with(len, || None);
        slots
    }
    let Column::Array(c) = col else { return None };
    match chain_terminal(&c.values)? {
        Column::Dictionary(dict) => Some(ChainCache::Dict(empty_slots(dict.values.len()))),
        Column::Dynamic(dynamic) => Some(ChainCache::Dynamic {
            contexts: empty_slots(dynamic.children.len()),
            shared: Vec::new(),
        }),
        Column::Json(_) => Some(ChainCache::Json(JsonChainCache::default())),
        _ => None,
    }
}

/// Build the cell at `index` as an owned pointer, None for a null cell. Used
/// by the row path, where columns interleave; the column paths hoist the
/// validity check instead.
///
/// # Safety
///
/// Returns an owned reference; the caller must take over the reference count.
unsafe fn column_value_to_owned_ptr<'py>(
    py: Python<'py>,
    col: &Column,
    ctx: &ColumnCtx<'py>,
    index: usize,
    cache: Option<&mut ChainCache<'py>>,
) -> PyResult<*mut ffi::PyObject> {
    if column_validity(col).is_some_and(|v| !v.is_valid(index)) {
        Ok(none_owned_ptr())
    } else {
        column_value_nonnull_ptr(py, col, ctx, index, cache)
    }
}
