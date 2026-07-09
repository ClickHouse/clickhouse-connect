use std::collections::HashMap;
use std::ffi::{c_char, c_long, CString};
use std::sync::Arc;

use pyo3::exceptions::{PyUnicodeDecodeError, PyValueError};
use pyo3::ffi;
use pyo3::intern;
use pyo3::prelude::*;
use pyo3::types::{PyCapsule, PyDate, PyDateTime, PyDict, PyList, PyString, PyTuple};

/// Wrapper to make ArrowArrayStream Send-safe for PyCapsule.
#[repr(transparent)]
struct SendableStream(core_ffi::ArrowArrayStream);
// Safety: the stream's private_data is a Box<StreamPrivateData> owning only
// Send + Sync data (Schema, Arc<ColBatch> chunks, CString), no Python objects
// or thread-affine state, so the capsule destructor may drop it on any thread.
unsafe impl Send for SendableStream {}

use ch_core_rs::batch::{ChunkedBatch, ColBatch as RustColBatch};
use ch_core_rs::bitmap::Bitmap;
use ch_core_rs::column::{Column, DecimalColumn, DictionaryColumn, MapColumn, TupleColumn};
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
            check_chunk_shape(chunk, num_cols)?;
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

            // Allocate every row tuple up front, moved into the list at once,
            // so the error path drops them through list_dealloc. Column-major
            // fill leaves NULL slots at later column indexes while an error
            // unwinds; tuple_dealloc tolerates them.
            for out_row in 0..total_rows {
                let tuple_ptr = ffi::PyTuple_New(num_cols as ffi::Py_ssize_t);
                if tuple_ptr.is_null() {
                    return Err(PyErr::fetch(py));
                }
                // Safety: out_row < total_rows, the list's allocated length;
                // the list takes over the owned tuple.
                ffi::PyList_SET_ITEM(list.as_ptr(), out_row as ffi::Py_ssize_t, tuple_ptr);
            }

            // Column-major fill: dispatch each column's type once per chunk
            // and write into slot col_idx of each row's tuple.
            let mut base: usize = 0;
            for chunk in &self.inner.chunks {
                let rows = chunk.num_rows;
                for (col_idx, (col, ctx)) in chunk.columns.iter().zip(&ctxs).enumerate() {
                    let mut sink = |i: usize, item: *mut ffi::PyObject| {
                        // Safety: base + i < total_rows, the chunk-row sum the
                        // list was filled to, so GET_ITEM borrows a live tuple;
                        // col_idx < num_cols, its allocated length; the tuple
                        // takes over the owned item.
                        let tuple =
                            ffi::PyList_GET_ITEM(list.as_ptr(), (base + i) as ffi::Py_ssize_t);
                        ffi::PyTuple_SET_ITEM(tuple, col_idx as ffi::Py_ssize_t, item);
                    };
                    fill_column(py, col, ctx, rows, &mut sink)?;
                }
                base += rows;
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
    /// `uuid.UUID` construction machinery, for a UUID column.
    uuid: Option<UuidCtx<'py>>,
    /// `ipaddress` class machinery, for an IPv4/IPv6 column.
    ip: Option<IpCtx<'py>>,
    /// The `decimal.Decimal` class, for a Decimal column.
    decimal_cls: Option<Bound<'py, PyAny>>,
    /// Recursively-prepared context for the element type of an Array column;
    /// `None` for any other type.
    element: Option<Box<ColumnCtx<'py>>>,
    /// Recursively-prepared per-field contexts. For a Tuple column, one per
    /// element in declaration order; for a Map column, exactly two (the key
    /// context then the value context). `None` for any other type.
    fields: Option<Vec<ColumnCtx<'py>>>,
    /// Pre-built element-name keys for a NAMED Tuple column, materialized as a
    /// `dict` keyed by these (clickhouse-connect's default read format).
    /// `None` for an unnamed Tuple (materialized as a `tuple`) and every
    /// non-Tuple type.
    tuple_names: Option<Vec<Bound<'py, PyString>>>,
}

/// Cached objects to build a `uuid.UUID` the way the Cython codec does:
/// allocate via `UUID.__new__` and set the fields with `object.__setattr__`,
/// bypassing the parsing constructor and the immutability guard.
struct UuidCtx<'py> {
    cls: Bound<'py, PyAny>,
    new: Bound<'py, PyAny>,
    object_setattr: Bound<'py, PyAny>,
    unsafe_marker: Bound<'py, PyAny>,
}

/// Cached objects to build an `ipaddress.IPv4Address`/`IPv6Address` via
/// `__new__` plus a plain `_ip` setattr (neither class guards setattr).
struct IpCtx<'py> {
    cls: Bound<'py, PyAny>,
    new: Bound<'py, PyAny>,
    /// `IPv6Address.__slots__` includes `_scope_id` (Python 3.9+); each value
    /// gets `_scope_id = None`.
    set_scope_id: bool,
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

    let (uuid, ip, decimal_cls) = match resolved {
        ChType::Uuid => {
            let module = py.import("uuid")?;
            let cls = module.getattr("UUID")?;
            let new = cls.getattr("__new__")?;
            let object_setattr = py
                .import("builtins")?
                .getattr("object")?
                .getattr("__setattr__")?;
            let unsafe_marker = module.getattr("SafeUUID")?.getattr("unsafe")?;
            (
                Some(UuidCtx {
                    cls,
                    new,
                    object_setattr,
                    unsafe_marker,
                }),
                None,
                None,
            )
        }
        ChType::Ipv4 => {
            let cls = py.import("ipaddress")?.getattr("IPv4Address")?;
            let new = cls.getattr("__new__")?;
            (
                None,
                Some(IpCtx {
                    cls,
                    new,
                    set_scope_id: false,
                }),
                None,
            )
        }
        ChType::Ipv6 => {
            let cls = py.import("ipaddress")?.getattr("IPv6Address")?;
            let new = cls.getattr("__new__")?;
            let set_scope_id = cls
                .getattr("__slots__")?
                .contains(intern!(py, "_scope_id"))?;
            (
                None,
                Some(IpCtx {
                    cls,
                    new,
                    set_scope_id,
                }),
                None,
            )
        }
        ChType::Decimal { .. } => (None, None, Some(py.import("decimal")?.getattr("Decimal")?)),
        _ => (None, None, None),
    };

    // `.inner()` only strips Nullable, so an Array column's resolved type is the
    // `Array(elem)` itself. Recurse into the element to build its machinery,
    // which transparently covers every element shape (Nullable, LowCardinality,
    // nested Array, temporal-with-tz, enum, uuid, ip, decimal).
    let element = match resolved {
        ChType::Array(elem) => Some(Box::new(prepare_column_ctx(py, elem)?)),
        _ => None,
    };

    // A Tuple builds one field context per element and, for a named tuple, the
    // pre-built name keys. A Map builds exactly two field contexts (key then
    // value); its entries live in a nested Tuple column reached directly, so it
    // needs no tuple_names. `resolved` is the Nullable-unwrapped type, so a
    // `Nullable(Tuple(...))` reaches the Tuple arm here.
    let (fields, tuple_names) = match resolved {
        ChType::Tuple(elements) => {
            let fields = elements
                .iter()
                .map(|(_, t)| prepare_column_ctx(py, t))
                .collect::<PyResult<Vec<_>>>()?;
            let named = !elements.is_empty() && elements.iter().all(|(name, _)| name.is_some());
            // `named` guarantees every element has a name; the empty-string
            // fallback keeps the map total (one key per field) without an
            // unwrap even though it is never taken.
            let names = if named {
                Some(
                    elements
                        .iter()
                        .map(|(name, _)| PyString::new(py, name.as_deref().unwrap_or_default()))
                        .collect(),
                )
            } else {
                None
            };
            (Some(fields), names)
        }
        ChType::Map(key, value) => {
            let fields = vec![prepare_column_ctx(py, key)?, prepare_column_ctx(py, value)?];
            (Some(fields), None)
        }
        _ => (None, None),
    };

    Ok(ColumnCtx {
        tz,
        fromtimestamp,
        precision,
        enum_names,
        uuid,
        ip,
        decimal_cls,
        element,
        fields,
        tuple_names,
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

/// Reject a chunk whose column count differs from the schema or whose column
/// lengths differ from the chunk row count. The core only debug_asserts these
/// invariants, so malformed payloads must fail here before any fill loop reads
/// a short buffer or Bool padding bits.
fn check_chunk_shape(chunk: &RustColBatch, num_cols: usize) -> PyResult<()> {
    if chunk.columns.len() != num_cols {
        return Err(PyValueError::new_err(format!(
            "Malformed payload: chunk has {} columns, expected {num_cols}",
            chunk.columns.len()
        )));
    }
    for (idx, col) in chunk.columns.iter().enumerate() {
        if col.len() != chunk.num_rows {
            return Err(PyValueError::new_err(format!(
                "Malformed payload: column {idx} has {} rows, chunk expects {}",
                col.len(),
                chunk.num_rows
            )));
        }
    }
    Ok(())
}

/// Build one column as a Python list across `chunks`, one owned pointer per
/// cell. Shared by `column_data` and `to_python_columns` so every path applies
/// one host-value policy.
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
        if chunk.columns[col_idx].len() != chunk.num_rows {
            return Err(PyValueError::new_err(format!(
                "Malformed payload: column {col_idx} has {} rows, chunk expects {}",
                chunk.columns[col_idx].len(),
                chunk.num_rows
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
            let base = out_row;
            let mut sink = |i: usize, item: *mut ffi::PyObject| {
                // Safety: base + i < total_rows, the chunk-row sum the list
                // was allocated with, and the list takes over the owned item.
                ffi::PyList_SET_ITEM(list.as_ptr(), (base + i) as ffi::Py_ssize_t, item);
            };
            fill_column(py, col, ctx, chunk.num_rows, &mut sink)?;
            out_row += chunk.num_rows;
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
/// Returns an owned reference; the caller must take over the reference count.
unsafe fn bool_owned_ptr(value: bool) -> *mut ffi::PyObject {
    let ptr = if value {
        ffi::Py_True()
    } else {
        ffi::Py_False()
    };
    ffi::Py_INCREF(ptr);
    ptr
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
        // Arrays carry no array-level validity; element nulls live on `values`.
        Column::Array(_) => None,
        // A Nullable(Tuple) carries tuple-level validity here; element nulls
        // live on the field columns. A plain Tuple has `validity == None`.
        Column::Tuple(c) => c.validity.as_ref(),
        // Maps are never nullable at the map level; value nulls live on the
        // values column inside `entries`.
        Column::Map(_) => None,
    }
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
unsafe fn fill_fixed_width<S>(
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
unsafe fn fill_dictionary<S>(
    py: Python<'_>,
    col: &DictionaryColumn,
    ctx: &ColumnCtx<'_>,
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

/// Error for a LowCardinality index outside its dictionary.
fn lc_index_err() -> PyErr {
    PyValueError::new_err("Malformed payload: LowCardinality index out of dictionary range")
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
/// Requires the GIL. `sink` is called exactly once per row in ascending row
/// order with an owned reference it must take over, and it must keep every
/// item alive until this call returns (the Tuple fill writes into containers
/// after sinking them).
unsafe fn fill_column<S>(
    py: Python<'_>,
    col: &Column,
    ctx: &ColumnCtx<'_>,
    rows: usize,
    sink: &mut S,
) -> PyResult<()>
where
    S: FnMut(usize, *mut ffi::PyObject),
{
    if fill_fixed_width(py, col, ctx, rows, sink)? {
        return Ok(());
    }
    match col {
        Column::Dictionary(dict) => fill_dictionary(py, dict, ctx, rows, sink),
        Column::Tuple(c) => fill_tuple(py, c, ctx, rows, sink),
        Column::Map(c) => fill_map(py, c, ctx, rows, sink),
        _ => {
            let mut dict_cache = new_array_dict_cache(col);
            match column_validity(col) {
                None => {
                    for i in 0..rows {
                        let item = column_value_nonnull_ptr(py, col, ctx, i, dict_cache.as_mut())?;
                        sink(i, item);
                    }
                }
                Some(bm) => {
                    for i in 0..rows {
                        let item = if bm.is_valid(i) {
                            column_value_nonnull_ptr(py, col, ctx, i, dict_cache.as_mut())?
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

/// Column-major fill for a Tuple column: allocate every row container up
/// front (tuple, presized dict, or None for a null row), hand each to `sink`,
/// then fill field by field through `fill_column`, one dispatch per field.
///
/// # Safety
///
/// Requires the GIL; `fill_column`'s sink contract applies. Containers are
/// filled after they reach the sink, so the sink keeping items alive until
/// this call returns is load-bearing.
unsafe fn fill_tuple(
    py: Python<'_>,
    c: &TupleColumn,
    ctx: &ColumnCtx<'_>,
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
unsafe fn fill_map(
    py: Python<'_>,
    c: &MapColumn,
    ctx: &ColumnCtx<'_>,
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

/// Materialize the first `rows` cells of `col` into owned objects through the
/// bulk fill machinery. Error paths drop whatever was already produced.
unsafe fn materialize_run(
    py: Python<'_>,
    col: &Column,
    ctx: &ColumnCtx<'_>,
    rows: usize,
) -> PyResult<Vec<Py<PyAny>>> {
    let mut out: Vec<Py<PyAny>> = Vec::with_capacity(rows);
    {
        let mut sink = |_i: usize, item: *mut ffi::PyObject| {
            // Safety: item is an owned reference the sink takes over.
            out.push(unsafe { Py::from_owned_ptr(py, item) });
        };
        let mut erased: DynSink<'_> = &mut sink;
        fill_column(py, col, ctx, rows, &mut erased)?;
    }
    if out.len() != rows {
        return Err(PyValueError::new_err(
            "internal error: column fill produced the wrong row count",
        ));
    }
    Ok(out)
}

/// Lazy cache of materialized dictionary slot objects, one entry per slot.
type DictSlotCache = Vec<Option<Py<PyAny>>>;

/// A cache for the Dictionary column in an Array column's element chain, if
/// any. One cache per array column per chunk, threaded through the per-cell
/// path so repeated LowCardinality labels materialize once and share the
/// object, matching `fill_dictionary`'s reuse policy. Slots fill lazily on
/// first reference by a valid index.
fn new_array_dict_cache(col: &Column) -> Option<DictSlotCache> {
    fn chain_dictionary(col: &Column) -> Option<&DictionaryColumn> {
        match col {
            Column::Array(c) => chain_dictionary(&c.values),
            Column::Dictionary(d) => Some(d),
            _ => None,
        }
    }
    let Column::Array(c) = col else { return None };
    chain_dictionary(&c.values).map(|dict| {
        let mut slots = Vec::with_capacity(dict.values.len());
        slots.resize_with(dict.values.len(), || None);
        slots
    })
}

/// Build the cell at `index` as an owned pointer, assuming the cell is not
/// null; callers check validity first. `dict_cache` is the Array element
/// chain's dictionary cache, if the caller materializes one.
///
/// # Safety
///
/// Returns an owned reference; the caller must take over the reference count.
unsafe fn column_value_nonnull_ptr(
    py: Python<'_>,
    col: &Column,
    ctx: &ColumnCtx<'_>,
    index: usize,
    mut dict_cache: Option<&mut DictSlotCache>,
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
            let slot = c
                .indices
                .get(index)
                .copied()
                .and_then(|i| usize::try_from(i).ok())
                .filter(|&slot| slot < c.values.len())
                .ok_or_else(lc_index_err)?;
            match dict_cache {
                // Array element path: build each referenced slot once per
                // chunk and emit clone_ref of the cached object.
                Some(cache) => {
                    let entry = cache.get_mut(slot).ok_or_else(lc_index_err)?;
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
                None => column_value_nonnull_ptr(py, &c.values, ctx, slot, None),
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
                    dict_cache.as_deref_mut(),
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
    }
}

/// Error for a column whose ColumnCtx was prepared for a different type; an
/// internal invariant violation, not a payload condition.
fn ctx_missing(what: &str) -> PyErr {
    PyValueError::new_err(format!("internal error: missing {what} column context"))
}

/// Error for a container column whose ColumnCtx carries a different number of
/// field contexts than the column has fields; an internal invariant violation.
fn ctx_count_mismatch(what: &str) -> PyErr {
    PyValueError::new_err(format!(
        "internal error: {what} field context count mismatch"
    ))
}

/// Error for an Array column whose offsets are out of range for the element
/// buffer (out of order, negative, or an end past the element count).
fn array_bounds_err() -> PyErr {
    PyValueError::new_err("Malformed payload: Array offsets are out of range")
}

/// Error for a Map column whose offsets are out of range for the entries
/// buffer (out of order, negative, or an end past the entry count).
fn map_bounds_err() -> PyErr {
    PyValueError::new_err("Malformed payload: Map offsets are out of range")
}

/// Error for a Tuple column whose field columns are shorter than the row count.
fn tuple_shape_err() -> PyErr {
    PyValueError::new_err("Malformed payload: Tuple field length mismatch")
}

/// Error for a Map column whose entries are not a two-field key/value tuple;
/// an internal invariant violation (the core always builds this shape).
fn map_entries_err() -> PyErr {
    PyValueError::new_err("internal error: Map entries are not a two-field tuple")
}

/// Error for a UUID/IPv6 cell whose fixed width is not 16 bytes.
fn fixed_width_err(what: &str) -> PyErr {
    PyValueError::new_err(format!("Malformed payload: {what} cell is not 16 bytes"))
}

/// Build a `uuid.UUID` from the 16 raw wire bytes: `UUID.__new__(UUID)`, then
/// `object.__setattr__` of `int` and `is_safe` (SafeUUID.unsafe), matching the
/// Cython codec's read_uuid_col. The wire int is le(b[0..8]) << 64 | le(b[8..16]),
/// which is `from_le_bytes` with the halves swapped.
fn uuid_value_ptr(py: Python<'_>, ctx: &UuidCtx<'_>, bytes: &[u8]) -> PyResult<*mut ffi::PyObject> {
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
fn ipv4_value_ptr(py: Python<'_>, ctx: &IpCtx<'_>, value: u32) -> PyResult<*mut ffi::PyObject> {
    let addr = ctx.new.call1((&ctx.cls,))?;
    addr.setattr(intern!(py, "_ip"), value)?;
    Ok(addr.into_ptr())
}

/// Build an `ipaddress.IPv6Address` from the 16 network-order wire bytes,
/// always IPv6Address even for a v4-mapped value, matching _read_binary_ip.
fn ipv6_value_ptr(py: Python<'_>, ctx: &IpCtx<'_>, bytes: &[u8]) -> PyResult<*mut ffi::PyObject> {
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
struct DecimalScratch {
    digits: String,
    text: String,
}

/// Build a `decimal.Decimal` for the cell: render the unscaled value as exact
/// decimal text (sign, integer digits, exactly `scale` fractional digits) and
/// call the class once. The text form yields the same value and exponent as
/// the python codec's `Decimal(unscaled).scaleb(-scale)`.
fn decimal_value_ptr(
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
    dict_cache: Option<&mut DictSlotCache>,
) -> PyResult<*mut ffi::PyObject> {
    if column_validity(col).is_some_and(|v| !v.is_valid(index)) {
        Ok(none_owned_ptr())
    } else {
        column_value_nonnull_ptr(py, col, ctx, index, dict_cache)
    }
}
