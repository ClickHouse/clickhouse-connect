use std::collections::HashMap;
use std::ffi::{c_char, CString};
use std::sync::Arc;

use pyo3::exceptions::{PyNotImplementedError, PyUnicodeDecodeError, PyValueError};
use pyo3::ffi;
use pyo3::prelude::*;
use pyo3::types::{PyCapsule, PyDate, PyDateTime, PyDict, PyList, PyString};

/// Wrapper to make ArrowArrayStream Send-safe for PyCapsule.
#[repr(transparent)]
struct SendableStream(core_ffi::ArrowArrayStream);
// Safety: the stream's private_data is a Box<StreamPrivateData> owning only
// Send + Sync data (Schema, Arc<ColBatch> chunks, CString), no Python objects
// or thread-affine state, so the capsule destructor may drop it on any thread.
unsafe impl Send for SendableStream {}

use ch_core_rs::batch::{ChunkedBatch, ColBatch as RustColBatch};
use ch_core_rs::bitmap::Bitmap;
use ch_core_rs::column::Column;
use ch_core_rs::ffi as core_ffi;
use ch_core_rs::native::decode::decode_all_bytes;
use ch_core_rs::schema::ChType;

use crate::decoder::{buffer_to_vec, decode_err, decode_options};

/// A query result: a sequence of decoded columnar chunks (one per Native
/// block), never concatenated. Exposes the chunks as a single Arrow C Stream
/// or materializes them into Python objects on demand.
#[pyclass(name = "ColBatch")]
pub struct ColBatch {
    inner: Arc<ChunkedBatch>,
}

impl ColBatch {
    pub(crate) fn from_chunked(inner: ChunkedBatch) -> Self {
        Self {
            inner: Arc::new(inner),
        }
    }

    /// Wrap a single decoded block as a one-chunk result. Used by the
    /// incremental decoders, which emit one block at a time.
    pub(crate) fn from_block(block: RustColBatch) -> Self {
        let schema = block.schema.clone();
        Self {
            inner: Arc::new(ChunkedBatch {
                schema,
                chunks: vec![Arc::new(block)],
            }),
        }
    }
}

#[pymethods]
impl ColBatch {
    /// Decode a complete Native payload from bytes, bytearray, or memoryview.
    #[staticmethod]
    #[pyo3(signature = (data, has_block_info = false))]
    fn decode_native(data: &Bound<'_, PyAny>, has_block_info: bool) -> PyResult<Self> {
        let options = decode_options(has_block_info);
        let chunked = if let Ok(bytes) = data.downcast::<pyo3::types::PyBytes>() {
            // bytes: decode straight from the borrowed buffer, no copy. The
            // borrow ties the decode to the GIL, so it cannot be released.
            decode_all_bytes(bytes.as_bytes(), &options)
        } else {
            // bytearray, memoryview: copy out, then decode without the GIL.
            let owned = buffer_to_vec(data)?;
            data.py()
                .allow_threads(|| decode_all_bytes(&owned, &options))
        }
        .map_err(decode_err)?;
        Ok(Self::from_chunked(chunked))
    }

    /// Merge already-decoded batches into one. The schema comes from the
    /// first batch and every later batch must match it. Chunks are shared via
    /// Arc clones; zero-row chunks are dropped, as in `decode_all_bytes`.
    #[staticmethod]
    fn from_batches(py: Python<'_>, batches: Vec<Py<ColBatch>>) -> PyResult<Self> {
        let first = batches
            .first()
            .ok_or_else(|| PyValueError::new_err("from_batches requires at least one batch"))?;
        let schema = first.borrow(py).inner.schema.clone();
        let mut chunks = Vec::new();
        for (i, batch) in batches.iter().enumerate() {
            let guard = batch.borrow(py);
            if guard.inner.schema != schema {
                return Err(PyValueError::new_err(format!(
                    "Batch {i} schema differs from the first batch"
                )));
            }
            chunks.extend(
                guard
                    .inner
                    .chunks
                    .iter()
                    .filter(|c| c.num_rows > 0)
                    .cloned(),
            );
        }
        Ok(Self::from_chunked(ChunkedBatch { schema, chunks }))
    }

    #[getter]
    fn num_rows(&self) -> usize {
        self.inner.num_rows()
    }

    #[getter]
    fn num_columns(&self) -> usize {
        self.inner.num_columns()
    }

    #[getter]
    fn num_chunks(&self) -> usize {
        self.inner.num_chunks()
    }

    #[getter]
    fn column_names(&self) -> Vec<String> {
        self.inner
            .schema
            .fields
            .iter()
            .map(|f| f.name.clone())
            .collect()
    }

    #[getter]
    fn column_type_names(&self) -> Vec<String> {
        self.inner
            .schema
            .fields
            .iter()
            .map(|f| f.ch_type.to_string())
            .collect()
    }

    /// Export all chunks as an Arrow C Stream capsule. `requested_schema` is
    /// accepted per the Arrow PyCapsule interface and ignored; the stream
    /// always carries its native schema.
    #[pyo3(signature = (requested_schema=None))]
    fn __arrow_c_stream__<'py>(
        &self,
        py: Python<'py>,
        requested_schema: Option<PyObject>,
    ) -> PyResult<Bound<'py, PyCapsule>> {
        let _ = requested_schema;

        // Safety: an all-zero ArrowArrayStream is a valid initial value: every
        // field is a raw pointer (null) or an Option<extern fn> whose None is
        // the all-zero bit pattern. export_chunks_to_stream overwrites every
        // field before the struct is observed.
        let mut stream: core_ffi::ArrowArrayStream = unsafe { std::mem::zeroed() };
        unsafe {
            // Safety: `stream` is a valid, writable, zeroed ArrowArrayStream.
            // Ownership of the exported data passes to its release callback.
            // Cheap: clones the chunk Vec (Arc clones), no column data copied.
            core_ffi::export_chunks_to_stream(
                self.inner.schema.clone(),
                self.inner.chunks.clone(),
                &mut stream,
            );
        }

        let name = CString::new("arrow_array_stream").unwrap();
        // The destructor frees the stream if the capsule is dropped unconsumed.
        // A consumer that imports the stream moves it out and clears `release`
        // in place, so the destructor sees `None` and does nothing.
        let capsule = PyCapsule::new_with_destructor(
            py,
            SendableStream(stream),
            Some(name),
            |mut stream: SendableStream, _context| {
                // Safety: `stream` was initialized by `export_chunks_to_stream`
                // and `release_if_set` is a no-op once a consumer cleared
                // `release`. Must not panic: pyo3's capsule destructor
                // trampoline has no unwind guard.
                unsafe { stream.0.release_if_set() }
            },
        )?;
        Ok(capsule)
    }

    /// Get column `index` as a single Python list, concatenated across chunks.
    fn column_data<'py>(&self, py: Python<'py>, index: usize) -> PyResult<Bound<'py, PyList>> {
        if index >= self.inner.num_columns() {
            return Err(PyValueError::new_err(format!(
                "Column index {index} out of range (0..{})",
                self.inner.num_columns()
            )));
        }

        let ctx = prepare_column_ctx(py, &self.inner.schema.fields[index].ch_type)?;
        column_to_pylist(py, &self.inner.chunks, index, &ctx)
    }

    /// Get all rows as a list of tuples, across all chunks.
    fn to_python_rows<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        let total_rows = self.inner.num_rows();
        let num_cols = self.inner.num_columns();
        // Resolve each column's temporal context once, not per cell, so a
        // tz-aware column imports its zoneinfo a single time for the whole table.
        let ctxs: Vec<ColumnCtx> = self
            .inner
            .schema
            .fields
            .iter()
            .map(|f| prepare_column_ctx(py, &f.ch_type))
            .collect::<PyResult<_>>()?;
        for chunk in &self.inner.chunks {
            check_chunk_width(chunk, num_cols)?;
        }
        unsafe {
            let list_ptr = ffi::PyList_New(total_rows as ffi::Py_ssize_t);
            if list_ptr.is_null() {
                return Err(PyErr::fetch(py));
            }
            // Safety: list_ptr came from PyList_New, so it is a list and this
            // is the sole owned reference. Binding it makes the error and
            // panic paths drop the list; list_dealloc tolerates NULL slots.
            let list = Bound::from_owned_ptr(py, list_ptr).downcast_into_unchecked::<PyList>();

            let mut out_row: usize = 0;
            for chunk in &self.inner.chunks {
                for row_idx in 0..chunk.num_rows {
                    let tuple_ptr = ffi::PyTuple_New(num_cols as ffi::Py_ssize_t);
                    if tuple_ptr.is_null() {
                        return Err(PyErr::fetch(py));
                    }
                    // Safety: sole owned reference, as for the list above;
                    // tuple_dealloc also tolerates NULL slots on early exit.
                    let tuple = Bound::from_owned_ptr(py, tuple_ptr);

                    for (col_idx, (col, ctx)) in chunk.columns.iter().zip(&ctxs).enumerate() {
                        let item = column_value_to_owned_ptr(py, col, ctx, row_idx)?;
                        // Safety: col_idx < num_cols, the tuple's allocated
                        // length, and the tuple takes over the owned item.
                        ffi::PyTuple_SET_ITEM(tuple.as_ptr(), col_idx as ffi::Py_ssize_t, item);
                    }

                    // Safety: out_row < total_rows, the list's allocated
                    // length; into_ptr moves the tuple's ownership to the list.
                    ffi::PyList_SET_ITEM(
                        list.as_ptr(),
                        out_row as ffi::Py_ssize_t,
                        tuple.into_ptr(),
                    );
                    out_row += 1;
                }
            }

            Ok(list)
        }
    }

    /// Get all columns as Python lists, each concatenated across chunks.
    fn to_python_columns<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        let cols: Vec<Bound<'py, PyList>> = (0..self.inner.num_columns())
            .map(|ci| {
                let ctx = prepare_column_ctx(py, &self.inner.schema.fields[ci].ch_type)?;
                column_to_pylist(py, &self.inner.chunks, ci, &ctx)
            })
            .collect::<PyResult<_>>()?;
        PyList::new(py, &cols)
    }
}

// ---------------------------------------------------------------------------
// Temporal value construction
//
// The core decodes temporal columns to their faithful native integer width and
// keeps timezone and precision in the schema's ChType, not in the buffers. The
// binding turns those integers into Python date/datetime objects here, matching
// the value policy of clickhouse-connect's Native reader:
//
//   Date, Date32                          -> datetime.date (naive)
//   DateTime / DateTime64, no tz or       -> datetime.datetime, no tzinfo,
//     a UTC-equivalent tz                     decoded as the UTC wall clock
//   DateTime / DateTime64, named non-UTC  -> datetime.datetime, tz-aware
//     tz                                      (zoneinfo.ZoneInfo)
//
// The naive path is pure epoch arithmetic with no per-row Python datetime
// parsing. The tz-aware path defers to datetime.fromtimestamp, which handles
// DST for the named zone.
// ---------------------------------------------------------------------------

/// Timezone names ClickHouse treats as UTC. A column in one of these renders as
/// a naive datetime, matching clickhouse-connect (tzutil.UTC_EQUIVALENTS).
const UTC_EQUIVALENTS: &[&str] = &[
    "UTC",
    "Etc/UTC",
    "UCT",
    "Etc/UCT",
    "GMT",
    "Etc/GMT",
    "GMT0",
    "GMT-0",
    "GMT+0",
    "Etc/GMT0",
    "Etc/GMT-0",
    "Etc/GMT+0",
    "Universal",
    "Etc/Universal",
    "Zulu",
    "Etc/Zulu",
    "Greenwich",
    "Etc/Greenwich",
];

/// Per-column context for materializing a column's host values, resolved once
/// per column rather than per cell. For a temporal column it carries the
/// timezone policy: a naive column (Date/Date32, or a DateTime/DateTime64 with
/// no timezone or a UTC-equivalent one) has `tz` `None` and is built by epoch
/// arithmetic, while a named non-UTC timezone holds its `zoneinfo.ZoneInfo` in
/// `tz` and the bound `datetime.datetime.fromtimestamp` in `fromtimestamp`. For
/// an Enum8/Enum16 column it carries `enum_names`, the value -> label-string map.
struct ColumnCtx<'py> {
    tz: Option<Bound<'py, PyAny>>,
    fromtimestamp: Option<Bound<'py, PyAny>>,
    /// DateTime64 fractional precision (decimal digits). 0 for Date/DateTime.
    precision: u8,
    /// Enum value -> pre-built label string, for an Enum8/Enum16 column; `None`
    /// for any other type. A value missing from the map materializes as None,
    /// matching clickhouse-connect's `int_map.get(value, None)`.
    enum_names: Option<HashMap<i64, Bound<'py, PyString>>>,
}

/// Build an enum's value -> label-string map, one Python str per variant created
/// once for the whole column.
fn enum_name_map<'py, V: Copy + Into<i64>>(
    py: Python<'py>,
    variants: &[(String, V)],
) -> HashMap<i64, Bound<'py, PyString>> {
    variants
        .iter()
        .map(|(name, value)| ((*value).into(), PyString::new(py, name)))
        .collect()
}

/// Resolve a column's ChType into a ColumnCtx. Cheap for the common naive case
/// (no Python calls); imports zoneinfo only for a named non-UTC zone and builds
/// the enum map only for an enum. Safe to call for any column type; types that
/// need neither yield the naive, non-enum default.
fn prepare_column_ctx<'py>(py: Python<'py>, ch_type: &ChType) -> PyResult<ColumnCtx<'py>> {
    // Unwrap LowCardinality to reach the value type before inner() strips any
    // Nullable, so LowCardinality(DateTime(tz)) still applies timezone policy.
    let resolved = match ch_type {
        ChType::LowCardinality(inner) => inner.inner(),
        other => other.inner(),
    };

    let enum_names = match resolved {
        ChType::Enum8 { variants } => Some(enum_name_map(py, variants)),
        ChType::Enum16 { variants } => Some(enum_name_map(py, variants)),
        _ => None,
    };

    let (timezone, precision) = match resolved {
        ChType::DateTime { timezone } => (timezone.as_deref(), 0u8),
        ChType::DateTime64 {
            precision,
            timezone,
        } => (timezone.as_deref(), *precision),
        _ => (None, 0u8),
    };

    let (tz, fromtimestamp) = match timezone {
        Some(tz) if !UTC_EQUIVALENTS.contains(&tz) => {
            let zone = py.import("zoneinfo")?.getattr("ZoneInfo")?.call1((tz,))?;
            let fromtimestamp = py
                .import("datetime")?
                .getattr("datetime")?
                .getattr("fromtimestamp")?;
            (Some(zone), Some(fromtimestamp))
        }
        _ => (None, None),
    };

    Ok(ColumnCtx {
        tz,
        fromtimestamp,
        precision,
        enum_names,
    })
}

/// Split DateTime64 ticks at `precision` into whole seconds and microseconds.
/// Euclidean division so pre-epoch (negative) ticks floor correctly and the
/// microsecond remainder stays in `0..1_000_000`. Sub-microsecond digits
/// (precision 7..9) are truncated, since Python datetime resolves to
/// microseconds, matching clickhouse-connect.
fn dt64_secs_micros(ticks: i64, precision: u8) -> (i64, u32) {
    let scale = 10i64.pow(precision as u32);
    let secs = ticks.div_euclid(scale);
    let frac = ticks.rem_euclid(scale);
    let micros = if precision <= 6 {
        frac * 10i64.pow(6 - precision as u32)
    } else {
        frac / 10i64.pow(precision as u32 - 6)
    };
    (secs, micros as u32)
}

/// Civil date (year, month, day) from a day count since 1970-01-01. Howard
/// Hinnant's `civil_from_days`; valid across the full Date/Date32 range. Month
/// and day are 1-based.
fn civil_from_days(days: i64) -> (i32, u8, u8) {
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097; // [0, 146096]
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365; // [0, 399]
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0, 365]
    let mp = (5 * doy + 2) / 153; // [0, 11]
    let d = (doy - (153 * mp + 2) / 5 + 1) as u8; // [1, 31]
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u8; // [1, 12]
    let year = (y + if m <= 2 { 1 } else { 0 }) as i32;
    (year, m, d)
}

/// Calendar components (year, month, day, hour, minute, second) from seconds
/// since 1970-01-01 00:00:00 UTC. Euclidean split keeps the time of day in
/// `0..86_400` for negative (pre-epoch) inputs.
fn civil_from_secs(secs: i64) -> (i32, u8, u8, u8, u8, u8) {
    let days = secs.div_euclid(86_400);
    let tod = secs.rem_euclid(86_400);
    let (year, month, day) = civil_from_days(days);
    let hour = (tod / 3600) as u8;
    let minute = ((tod % 3600) / 60) as u8;
    let second = (tod % 60) as u8;
    (year, month, day, hour, minute, second)
}

/// Build a `datetime.date` from a day count since the epoch.
fn make_date(py: Python<'_>, days: i64) -> PyResult<Bound<'_, PyAny>> {
    let (year, month, day) = civil_from_days(days);
    Ok(PyDate::new(py, year, month, day)?.into_any())
}

/// Build a `datetime.datetime` from epoch seconds plus microseconds, honoring
/// the column's ColumnCtx (naive UTC arithmetic, or tz-aware fromtimestamp).
fn make_datetime<'py>(
    py: Python<'py>,
    secs: i64,
    micros: u32,
    ctx: &ColumnCtx<'py>,
) -> PyResult<Bound<'py, PyAny>> {
    match (&ctx.tz, &ctx.fromtimestamp) {
        (Some(tz), Some(fromtimestamp)) => {
            if micros == 0 {
                fromtimestamp.call1((secs, tz))
            } else if secs.unsigned_abs() < (1 << 32) {
                // Single call on a float timestamp. Exact: |secs| < 2^32 keeps
                // the f64 ulp of the sum under one microsecond, so CPython's
                // round-to-nearest-microsecond recovers `micros` exactly.
                fromtimestamp.call1((secs as f64 + micros as f64 / 1e6, tz))
            } else {
                // Distant timestamps lose sub-microsecond float precision;
                // set the exact microsecond on the aware datetime instead.
                let dt = fromtimestamp.call1((secs, tz))?;
                let kwargs = PyDict::new(py);
                kwargs.set_item("microsecond", micros)?;
                dt.call_method("replace", (), Some(&kwargs))
            }
        }
        _ => {
            let (year, month, day, hour, minute, second) = civil_from_secs(secs);
            Ok(
                PyDateTime::new(py, year, month, day, hour, minute, second, micros, None)?
                    .into_any(),
            )
        }
    }
}

/// Build one column as a Python list across `chunks`, one owned pointer per
/// cell via `column_value_to_owned_ptr`. Shared by `column_data` and
/// `to_python_columns` so every path applies one host-value policy.
/// Reject a chunk whose column count differs from the schema. The core does
/// not validate later blocks against the first block's schema, so malformed
/// multi-block payloads must fail here before any raw list or tuple fill.
fn check_chunk_width(chunk: &RustColBatch, num_cols: usize) -> PyResult<()> {
    if chunk.columns.len() != num_cols {
        return Err(PyValueError::new_err(format!(
            "Malformed payload: chunk has {} columns, expected {num_cols}",
            chunk.columns.len()
        )));
    }
    Ok(())
}

fn column_to_pylist<'py>(
    py: Python<'py>,
    chunks: &[Arc<RustColBatch>],
    col_idx: usize,
    ctx: &ColumnCtx<'_>,
) -> PyResult<Bound<'py, PyList>> {
    for chunk in chunks {
        if col_idx >= chunk.columns.len() {
            return Err(PyValueError::new_err(format!(
                "Malformed payload: chunk has {} columns, expected at least {}",
                chunk.columns.len(),
                col_idx + 1
            )));
        }
    }
    let total_rows: usize = chunks.iter().map(|c| c.num_rows).sum();
    unsafe {
        let list_ptr = ffi::PyList_New(total_rows as ffi::Py_ssize_t);
        if list_ptr.is_null() {
            return Err(PyErr::fetch(py));
        }
        // Safety: list_ptr came from PyList_New, so it is a list and this is
        // the sole owned reference. Binding it makes the error and panic paths
        // drop the list; list_dealloc tolerates the NULL slots not yet filled.
        let list = Bound::from_owned_ptr(py, list_ptr).downcast_into_unchecked::<PyList>();

        let mut out_row: usize = 0;
        for chunk in chunks {
            let col = &chunk.columns[col_idx];
            // Hoist the validity branch out of the per-cell loop; most
            // columns are non-nullable.
            match column_validity(col) {
                None => {
                    for row_idx in 0..chunk.num_rows {
                        let item = column_value_nonnull_ptr(py, col, ctx, row_idx)?;
                        // Safety: out_row < total_rows, the same chunk-row sum
                        // the list was allocated with, and the list takes over
                        // the owned item.
                        ffi::PyList_SET_ITEM(list.as_ptr(), out_row as ffi::Py_ssize_t, item);
                        out_row += 1;
                    }
                }
                Some(bm) => {
                    for row_idx in 0..chunk.num_rows {
                        let item = if bm.is_valid(row_idx) {
                            column_value_nonnull_ptr(py, col, ctx, row_idx)?
                        } else {
                            none_owned_ptr()
                        };
                        // Safety: as above.
                        ffi::PyList_SET_ITEM(list.as_ptr(), out_row as ffi::Py_ssize_t, item);
                        out_row += 1;
                    }
                }
            }
        }

        Ok(list)
    }
}

/// Build a Python str from raw String column bytes. Invalid UTF-8 renders as
/// the lowercase hex of the raw bytes, matching clickhouse-connect's String
/// read fallback. Single scan in the valid case: CPython's decode is the
/// validation, and the hex path runs only after a UnicodeDecodeError.
fn utf8_or_hex_owned_ptr(py: Python<'_>, bytes: &[u8]) -> PyResult<*mut ffi::PyObject> {
    // Safety: the pointer/length pair is a live borrowed slice and CPython
    // copies the bytes before returning. A zero-length slice's dangling
    // pointer is never read.
    let ptr = unsafe {
        ffi::PyUnicode_FromStringAndSize(
            bytes.as_ptr() as *const c_char,
            bytes.len() as ffi::Py_ssize_t,
        )
    };
    if !ptr.is_null() {
        return Ok(ptr);
    }
    let err = PyErr::fetch(py);
    if !err.is_instance_of::<PyUnicodeDecodeError>(py) {
        return Err(err);
    }
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut hex = Vec::with_capacity(bytes.len() * 2);
    for &b in bytes {
        hex.push(HEX[(b >> 4) as usize]);
        hex.push(HEX[(b & 0x0f) as usize]);
    }
    // Safety: hex is a live ASCII buffer, valid UTF-8, copied by CPython.
    unsafe {
        ptr_to_result(
            py,
            ffi::PyUnicode_FromStringAndSize(
                hex.as_ptr() as *const c_char,
                hex.len() as ffi::Py_ssize_t,
            ),
        )
    }
}

/// # Safety
///
/// Returns an owned reference; the caller must take over the reference count.
unsafe fn none_owned_ptr() -> *mut ffi::PyObject {
    let none = ffi::Py_None();
    ffi::Py_INCREF(none);
    none
}

/// # Safety
///
/// `ptr` must be an owned reference or null; on `Ok` the caller takes over
/// the reference count.
unsafe fn ptr_to_result(py: Python<'_>, ptr: *mut ffi::PyObject) -> PyResult<*mut ffi::PyObject> {
    if ptr.is_null() {
        Err(PyErr::fetch(py))
    } else {
        Ok(ptr)
    }
}

/// The column's validity bitmap, if any.
fn column_validity(col: &Column) -> Option<&Bitmap> {
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
        Column::Date(c) => c.validity.as_ref(),
        Column::Date32(c) => c.validity.as_ref(),
        Column::DateTime(c) => c.validity.as_ref(),
        Column::DateTime64(c) => c.validity.as_ref(),
        Column::Utf8(c) => c.validity.as_ref(),
        Column::FixedBinary(c) => c.validity.as_ref(),
        Column::Ipv4(c) => c.validity.as_ref(),
        Column::Ipv6(c) => c.validity.as_ref(),
        Column::Uuid(c) => c.validity.as_ref(),
        Column::Enum8(c) => c.validity.as_ref(),
        Column::Enum16(c) => c.validity.as_ref(),
        // A LowCardinality column's nulls live in the index validity, the Arrow
        // dictionary convention, not as a dictionary entry.
        Column::Dictionary(c) => c.validity.as_ref(),
        Column::Decimal(c) => c.validity.as_ref(),
    }
}

/// Build the cell at `index` as an owned pointer, assuming the cell is not
/// null; callers check validity first.
///
/// # Safety
///
/// Returns an owned reference; the caller must take over the reference count.
unsafe fn column_value_nonnull_ptr(
    py: Python<'_>,
    col: &Column,
    ctx: &ColumnCtx<'_>,
    index: usize,
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
        Column::Float32(c) => ptr_to_result(py, ffi::PyFloat_FromDouble(c.values[index].into())),
        Column::Float64(c) => ptr_to_result(py, ffi::PyFloat_FromDouble(c.values[index])),
        Column::Date(c) => Ok(make_date(py, c.values[index] as i64)?.into_ptr()),
        Column::Date32(c) => Ok(make_date(py, c.values[index] as i64)?.into_ptr()),
        Column::DateTime(c) => Ok(make_datetime(py, c.values[index] as i64, 0, ctx)?.into_ptr()),
        Column::DateTime64(c) => {
            let (secs, micros) = dt64_secs_micros(c.values[index], ctx.precision);
            Ok(make_datetime(py, secs, micros, ctx)?.into_ptr())
        }
        Column::Utf8(c) => utf8_or_hex_owned_ptr(py, c.value(index)),
        Column::FixedBinary(c) => {
            let bytes = c.value(index);
            ptr_to_result(
                py,
                ffi::PyBytes_FromStringAndSize(
                    bytes.as_ptr() as *const c_char,
                    bytes.len() as ffi::Py_ssize_t,
                ),
            )
        }
        // LowCardinality(T): resolve the row's dictionary index, then build the
        // inner value through this same constructor. The cell is known non-null
        // here (callers check the index validity first), so the resolved slot is
        // a real dictionary entry. The ctx already reflects the inner type, so a
        // LowCardinality temporal column gets the right timezone and precision.
        Column::Dictionary(c) => {
            let slot = c.indices[index] as usize;
            column_value_nonnull_ptr(py, &c.values, ctx, slot)
        }
        // Enum8/Enum16 carry only the physical signed int; map it to its label
        // string through the per-column value->name map. A value with no defined
        // label becomes None, matching clickhouse-connect's int_map.get default.
        Column::Enum8(c) => enum_value_ptr(ctx, c.values[index] as i64),
        Column::Enum16(c) => enum_value_ptr(ctx, c.values[index] as i64),
        // Decoded by the core and exportable through the Arrow exit, but the
        // Python object value policy for these is not implemented yet.
        Column::Ipv4(_)
        | Column::Ipv6(_)
        | Column::Uuid(_)
        | Column::Decimal(_) => Err(PyNotImplementedError::new_err(
            "this column type is not yet supported on the Python object exit; use the Arrow exit",
        )),
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

/// Build the cell at `index` as an owned pointer, None for a null cell. Used
/// by the row path, where columns interleave; the column paths hoist the
/// validity check instead.
///
/// # Safety
///
/// Returns an owned reference; the caller must take over the reference count.
unsafe fn column_value_to_owned_ptr(
    py: Python<'_>,
    col: &Column,
    ctx: &ColumnCtx<'_>,
    index: usize,
) -> PyResult<*mut ffi::PyObject> {
    if column_validity(col).is_some_and(|v| !v.is_valid(index)) {
        Ok(none_owned_ptr())
    } else {
        column_value_nonnull_ptr(py, col, ctx, index)
    }
}
