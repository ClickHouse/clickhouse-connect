use std::ffi::CString;
use std::sync::Arc;

use pyo3::exceptions::PyValueError;
use pyo3::ffi;
use pyo3::prelude::*;
use pyo3::types::{PyAny, PyBytes, PyCapsule, PyList, PyModule};

use ch_core_rs::batch::{ChunkedBatch, ColBatch as RustColBatch};
use ch_core_rs::column::Column;
use ch_core_rs::ffi as core_ffi;
use ch_core_rs::native::decode::decode_all_bytes;
use ch_core_rs::schema::ChType;

use crate::decoder::{buffer_to_vec, decode_err, decode_options};
use crate::pyval::{fill_column, prepare_column_ctx, ColumnCtx};

/// Wrapper to make ArrowArrayStream Send-safe for PyCapsule.
#[repr(transparent)]
struct SendableStream(core_ffi::ArrowArrayStream);
// Safety: the stream's private_data is a Box<StreamPrivateData> owning only
// Send + Sync data (Schema, Arc<ColBatch> chunks, CString), no Python objects
// or thread-affine state, so the capsule destructor may drop it on any thread.
unsafe impl Send for SendableStream {}

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
    #[pyo3(signature = (index, raw_time_ticks = false))]
    fn column_data<'py>(
        &self,
        py: Python<'py>,
        index: usize,
        raw_time_ticks: bool,
    ) -> PyResult<Bound<'py, PyList>> {
        if index >= self.inner.num_columns() {
            return Err(PyValueError::new_err(format!(
                "Column index {index} out of range (0..{})",
                self.inner.num_columns()
            )));
        }

        let ctx = prepare_column_ctx(py, &self.inner.schema.fields[index].ch_type, raw_time_ticks)?;
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
            .map(|f| prepare_column_ctx(py, &f.ch_type, false))
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

    /// Get all columns as Python containers, each concatenated across chunks.
    ///
    /// With `typed_numeric`, top-level non-nullable fixed-width numeric columns
    /// use `array.array`, matching clickhouse-connect's Python Native decoder.
    #[pyo3(signature = (*, typed_numeric = false))]
    fn to_python_columns<'py>(
        &self,
        py: Python<'py>,
        typed_numeric: bool,
    ) -> PyResult<Bound<'py, PyList>> {
        for chunk in &self.inner.chunks {
            check_chunk_shape(chunk, self.inner.num_columns())?;
        }
        let array_ctor = if typed_numeric {
            Some(PyModule::import(py, "array")?.getattr("array")?)
        } else {
            None
        };
        let cols: Vec<Bound<'py, PyAny>> = (0..self.inner.num_columns())
            .map(|ci| {
                let ch_type = &self.inner.schema.fields[ci].ch_type;
                if let Some(ctor) = &array_ctor {
                    if let Some(column) =
                        typed_numeric_column(py, &self.inner.chunks, ci, ch_type, ctor)?
                    {
                        return Ok(column);
                    }
                }
                let ctx = prepare_column_ctx(py, ch_type, false)?;
                Ok(column_to_pylist(py, &self.inner.chunks, ci, &ctx)?.into_any())
            })
            .collect::<PyResult<_>>()?;
        PyList::new(py, &cols)
    }
}

/// Build a Python `array.array` directly from native primitive buffers.
///
/// The Rust vectors and `array.array` both use host-native byte order. The
/// binding only ships on the mainstream CPython platforms where the selected
/// typecodes have the same widths as their Rust primitives.
fn typed_numeric_column<'py>(
    py: Python<'py>,
    chunks: &[Arc<RustColBatch>],
    col_idx: usize,
    ch_type: &ChType,
    array_ctor: &Bound<'py, PyAny>,
) -> PyResult<Option<Bound<'py, PyAny>>> {
    let Some(first) = chunks.first() else {
        // Zero-row result: no chunks, so the schema type picks the typecode.
        // Aliases (SimpleAggregateFunction) resolve to the physical type the
        // non-empty case would have matched on.
        let resolved;
        let ch_type = match ch_type.physical_delegate() {
            Some(delegate) => {
                resolved = delegate;
                &resolved
            }
            None => ch_type,
        };
        let typecode = match ch_type {
            ChType::Int8 => "b",
            ChType::Int16 => "h",
            ChType::Int32 => "i",
            ChType::Int64 => "q",
            ChType::UInt8 => "B",
            ChType::UInt16 => "H",
            ChType::UInt32 => "I",
            ChType::UInt64 => "Q",
            ChType::Float32 => "f",
            ChType::Float64 => "d",
            _ => return Ok(None),
        };
        return Ok(Some(array_ctor.call1((typecode,))?));
    };

    macro_rules! build_array {
        ($variant:ident, $typecode:literal, $ty:ty) => {{
            let output = array_ctor.call1(($typecode,))?;
            debug_assert_eq!(
                output.getattr("itemsize")?.extract::<usize>()?,
                std::mem::size_of::<$ty>(),
                "array.array typecode width differs from the Rust primitive"
            );
            for chunk in chunks {
                let Column::$variant(column) = &chunk.columns[col_idx] else {
                    return Ok(None);
                };
                if column.validity.is_some() {
                    return Ok(None);
                }
                let byte_len = std::mem::size_of_val(column.values.as_slice());
                // Safety: primitive Vec storage is contiguous and live for this
                // call. Reinterpreting it as bytes preserves its native layout.
                let bytes = unsafe {
                    std::slice::from_raw_parts(column.values.as_ptr().cast::<u8>(), byte_len)
                };
                output.call_method1("frombytes", (PyBytes::new(py, bytes),))?;
            }
            Ok(Some(output))
        }};
    }

    match &first.columns[col_idx] {
        Column::Int8(column) if column.validity.is_none() => build_array!(Int8, "b", i8),
        Column::Int16(column) if column.validity.is_none() => build_array!(Int16, "h", i16),
        Column::Int32(column) if column.validity.is_none() => build_array!(Int32, "i", i32),
        Column::Int64(column) if column.validity.is_none() => build_array!(Int64, "q", i64),
        Column::UInt8(column) if column.validity.is_none() => build_array!(UInt8, "B", u8),
        Column::UInt16(column) if column.validity.is_none() => build_array!(UInt16, "H", u16),
        Column::UInt32(column) if column.validity.is_none() => build_array!(UInt32, "I", u32),
        Column::UInt64(column) if column.validity.is_none() => build_array!(UInt64, "Q", u64),
        Column::Float32(column) if column.validity.is_none() => build_array!(Float32, "f", f32),
        Column::Float64(column) if column.validity.is_none() => build_array!(Float64, "d", f64),
        _ => Ok(None),
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
