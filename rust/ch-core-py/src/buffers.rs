//! Private, read-only buffer exports over immutable decoded chunks.

use std::ffi::{c_int, c_void};
use std::sync::Arc;

use ch_core_rs::batch::{ChunkedBatch, ColBatch};
use ch_core_rs::bitmap::Bitmap;
use ch_core_rs::column::Column;
use ch_core_rs::schema::ChType;
use pyo3::exceptions::{PyBufferError, PyValueError};
use pyo3::ffi;
use pyo3::prelude::*;

/// Physical metadata stays separate from the ClickHouse logical schema.
#[pyclass(name = "_ColumnBuffers", frozen, get_all)]
pub(crate) struct ColumnBuffers {
    kind: &'static str,
    itemsize: usize,
    byteorder: &'static str,
    length: usize,
    null_count: usize,
    values: Py<Buffer>,
    validity: Option<Py<Buffer>>,
}

#[derive(Clone, Copy)]
enum Selection {
    Values,
    Validity,
}

/// No pointers or Python references are stored here. An exported view pins
/// exactly one immutable chunk, including its other columns.
#[pyclass(name = "_Buffer", frozen, weakref)]
struct Buffer {
    chunk: Arc<ColBatch>,
    index: usize,
    selection: Selection,
}

#[pymethods]
impl Buffer {
    unsafe fn __getbuffer__(
        slf: Bound<'_, Self>,
        view: *mut ffi::Py_buffer,
        flags: c_int,
    ) -> PyResult<()> {
        if view.is_null() {
            return Err(PyBufferError::new_err("Buffer view is null"));
        }
        // Safety: CPython supplies a writable Py_buffer. The buffer protocol
        // requires obj=NULL on every error. FillInfo doesn't set it when a
        // writable request is rejected, so clear it before any fallible work.
        unsafe { (*view).obj = std::ptr::null_mut() };
        let data = slf.get().data().map_err(PyBufferError::new_err)?;
        // Safety: CPython supplies a writable Py_buffer. The pointer covers
        // `len` initialized bytes in an immutable Vec owned by slf's Arc.
        // Empty Vec pointers are non-null and never dereferenced for len=0.
        // FillInfo rejects writable requests and holds its own reference to
        // slf until PyBuffer_Release. No per-export allocations need freeing.
        let result = unsafe {
            ffi::PyBuffer_FillInfo(view, slf.as_ptr(), data.ptr.cast_mut(), data.len, 1, flags)
        };
        if result == -1 {
            return Err(PyErr::fetch(slf.py()));
        }
        Ok(())
    }
}

impl Buffer {
    fn data(&self) -> Result<BufferData, &'static str> {
        let column = self
            .chunk
            .columns
            .get(self.index)
            .ok_or("Missing buffer column")?;
        let numeric = numeric_column(column).ok_or("Unsupported buffer storage")?;
        match self.selection {
            Selection::Values => numeric.values,
            Selection::Validity => {
                let bitmap = numeric.validity.ok_or("Missing validity buffer")?;
                validity_buffer(bitmap)
            }
        }
    }
}

struct BufferData {
    ptr: *const c_void,
    len: isize,
}

impl BufferData {
    fn new(ptr: *const c_void, count: usize, itemsize: usize) -> Result<Self, &'static str> {
        let bytes = count
            .checked_mul(itemsize)
            .and_then(|n| isize::try_from(n).ok())
            .ok_or("Buffer byte length exceeds Py_ssize_t")?;
        Ok(Self { ptr, len: bytes })
    }
}

fn validity_buffer(bitmap: &Bitmap) -> Result<BufferData, &'static str> {
    let bytes = bitmap
        .as_bytes()
        .get(..bitmap.len().div_ceil(8))
        .ok_or("Validity buffer storage is shorter than its logical length")?;
    BufferData::new(bytes.as_ptr().cast(), bytes.len(), 1)
}

struct NumericColumn<'a> {
    kind: &'static str,
    itemsize: usize,
    length: usize,
    values: Result<BufferData, &'static str>,
    validity: Option<&'a Bitmap>,
}

fn numeric_column(column: &Column) -> Option<NumericColumn<'_>> {
    macro_rules! numeric {
        ($column:expr, $kind:literal, $ty:ty) => {{
            let values = $column.values.as_slice();
            let itemsize = std::mem::size_of::<$ty>();
            NumericColumn {
                kind: $kind,
                itemsize,
                length: values.len(),
                values: BufferData::new(values.as_ptr().cast(), values.len(), itemsize),
                validity: $column.validity.as_ref(),
            }
        }};
    }
    Some(match column {
        Column::Int8(c) => numeric!(c, "int8", i8),
        Column::Int16(c) => numeric!(c, "int16", i16),
        Column::Int32(c) => numeric!(c, "int32", i32),
        Column::Int64(c) => numeric!(c, "int64", i64),
        Column::UInt8(c) => numeric!(c, "uint8", u8),
        Column::UInt16(c) => numeric!(c, "uint16", u16),
        Column::UInt32(c) => numeric!(c, "uint32", u32),
        Column::UInt64(c) => numeric!(c, "uint64", u64),
        Column::Float32(c) => numeric!(c, "float32", f32),
        Column::Float64(c) => numeric!(c, "float64", f64),
        _ => return None,
    })
}

fn numeric_kind(ch_type: &ChType) -> Option<&'static str> {
    Some(match ch_type {
        ChType::Int8 => "int8",
        ChType::Int16 => "int16",
        ChType::Int32 => "int32",
        ChType::Int64 => "int64",
        ChType::UInt8 => "uint8",
        ChType::UInt16 => "uint16",
        ChType::UInt32 => "uint32",
        ChType::UInt64 => "uint64",
        ChType::Float32 => "float32",
        ChType::Float64 => "float64",
        ChType::Nullable(inner) | ChType::SimpleAggregateFunction { inner, .. } => {
            return numeric_kind(inner);
        }
        _ => return None,
    })
}

fn validate_numeric(
    column: &NumericColumn<'_>,
    kind: &str,
    rows: usize,
) -> Result<(), &'static str> {
    if column.kind != kind {
        return Err("Numeric buffer storage differs from the schema");
    }
    if column.length != rows {
        return Err("Numeric buffer length differs from the chunk row count");
    }
    column.values.as_ref().map_err(|err| *err)?;
    if let Some(bitmap) = column.validity {
        if bitmap.len() != rows {
            return Err("Validity buffer length differs from the column length");
        }
        validity_buffer(bitmap)?;
    }
    Ok(())
}

pub(crate) fn column_buffers(
    py: Python<'_>,
    batch: &ChunkedBatch,
    index: usize,
) -> PyResult<Option<Vec<ColumnBuffers>>> {
    let field = batch.schema.fields.get(index).ok_or_else(|| {
        PyValueError::new_err(format!(
            "Column index {index} out of range (0..{})",
            batch.num_columns()
        ))
    })?;
    let Some(kind) = numeric_kind(&field.ch_type) else {
        return Ok(None);
    };
    let mut descriptors = Vec::with_capacity(batch.chunks.len());
    for chunk in &batch.chunks {
        if chunk.columns.len() != batch.num_columns() {
            return Err(PyValueError::new_err(
                "Chunk column count differs from the schema",
            ));
        }
        let numeric = numeric_column(&chunk.columns[index]).ok_or_else(|| {
            PyValueError::new_err("Numeric buffer storage differs from the schema")
        })?;
        validate_numeric(&numeric, kind, chunk.num_rows).map_err(PyValueError::new_err)?;
        let owner = |selection| {
            Py::new(
                py,
                Buffer {
                    chunk: Arc::clone(chunk),
                    index,
                    selection,
                },
            )
        };
        descriptors.push(ColumnBuffers {
            kind,
            itemsize: numeric.itemsize,
            byteorder: if cfg!(target_endian = "little") {
                "little"
            } else {
                "big"
            },
            length: numeric.length,
            null_count: numeric.validity.map_or(0, Bitmap::null_count),
            values: owner(Selection::Values)?,
            validity: numeric
                .validity
                .map(|_| owner(Selection::Validity))
                .transpose()?,
        });
    }
    Ok(Some(descriptors))
}

#[cfg(test)]
mod tests {
    use super::*;
    use ch_core_rs::column::PrimitiveColumn;
    use ch_core_rs::schema::{Field, Schema};
    use pyo3::types::{PyMemoryView, PySlice};

    fn chunk(values: Vec<i64>) -> Arc<ColBatch> {
        let rows = values.len();
        Arc::new(ColBatch::new(
            Schema::new(vec![Field {
                name: "v".into(),
                ch_type: ChType::Int64,
            }]),
            vec![Column::Int64(PrimitiveColumn::new(values))],
            rows,
        ))
    }

    #[test]
    fn validates_numeric_metadata_and_lengths() {
        let column = Column::Int64(PrimitiveColumn::new(vec![13, 79]));
        let numeric = numeric_column(&column).unwrap();
        assert!(validate_numeric(&numeric, "int64", 2).is_ok());
        assert!(validate_numeric(&numeric, "uint64", 2).is_err());
        assert!(validate_numeric(&numeric, "int64", 3).is_err());
        let short_bitmap = Column::Int64(PrimitiveColumn::new_nullable(
            vec![13, 79],
            Bitmap::all_valid(1),
        ));
        assert!(validate_numeric(&numeric_column(&short_bitmap).unwrap(), "int64", 2).is_err());
        let ptr = std::ptr::NonNull::<u8>::dangling().as_ptr().cast();
        assert!(BufferData::new(ptr, usize::MAX, 8).is_err());
        assert!(BufferData::new(ptr, isize::MAX as usize + 1, 1).is_err());
    }

    #[test]
    fn validity_export_uses_logical_byte_length() {
        for rows in [0_usize, 7, 8, 9, 64, 65] {
            let byte_length = rows.div_ceil(8);
            let bitmap = Bitmap::from_raw(vec![0xff; byte_length + 13], rows);
            let expected_ptr = bitmap.as_bytes().as_ptr().cast();
            let column = Column::Int64(PrimitiveColumn::new_nullable(vec![13; rows], bitmap));
            assert!(validate_numeric(&numeric_column(&column).unwrap(), "int64", rows).is_ok());
            let buffer = Buffer {
                chunk: Arc::new(ColBatch::new(
                    Schema::new(vec![Field {
                        name: "v".into(),
                        ch_type: ChType::Nullable(Box::new(ChType::Int64)),
                    }]),
                    vec![column],
                    rows,
                )),
                index: 0,
                selection: Selection::Validity,
            };
            let data = buffer.data().unwrap();
            assert_eq!(data.len as usize, byte_length, "rows={rows}");
            assert_eq!(data.ptr, expected_ptr);
        }
    }

    // The core constructor debug-asserts this invariant but permits malformed
    // bitmaps in release builds. Validate before null_count can index them.
    #[cfg_attr(
        debug_assertions,
        ignore = "The core constructor debug-asserts bitmap storage length"
    )]
    #[test]
    fn rejects_short_bitmap_storage() {
        let column = Column::Int64(PrimitiveColumn::new_nullable(
            vec![13, 79],
            Bitmap::from_raw(vec![], 2),
        ));
        assert!(validate_numeric(&numeric_column(&column).unwrap(), "int64", 2).is_err());
    }

    #[test]
    fn rejects_null_and_writable_views_without_retaining_owner() {
        Python::initialize();
        Python::attach(|py| {
            let owner = Bound::new(
                py,
                Buffer {
                    chunk: chunk(vec![13]),
                    index: 0,
                    selection: Selection::Values,
                },
            )
            .unwrap();
            // Safety: owner is a live Python object on the attached thread.
            let refs = unsafe { ffi::Py_REFCNT(owner.as_ptr()) };
            // Safety: the implementation explicitly rejects a null view.
            assert!(unsafe {
                Buffer::__getbuffer__(owner.clone(), std::ptr::null_mut(), ffi::PyBUF_SIMPLE)
            }
            .is_err());
            // Safety: Py_buffer contains only integers and pointers, all of
            // which admit zero initialization. The failed export owns nothing.
            let mut view: ffi::Py_buffer = unsafe { std::mem::zeroed() };
            view.obj = 13_usize as *mut ffi::PyObject;
            // Safety: view is a valid writable buffer struct. A writable
            // request must fail before acquiring an owning reference.
            let error =
                unsafe { Buffer::__getbuffer__(owner.clone(), &mut view, ffi::PyBUF_WRITABLE) }
                    .unwrap_err();
            assert!(error.is_instance_of::<PyBufferError>(py));
            assert!(view.obj.is_null());
            // Safety: owner is still live, including after failed exports.
            assert_eq!(unsafe { ffi::Py_REFCNT(owner.as_ptr()) }, refs);
        });
    }

    #[test]
    fn view_pins_only_source_chunk_until_final_release() {
        Python::initialize();
        Python::attach(|py| {
            let first = chunk(vec![13, 79]);
            let second = chunk(vec![97]);
            let first_weak = Arc::downgrade(&first);
            let second_weak = Arc::downgrade(&second);
            let expected_ptr = match &first.columns[0] {
                Column::Int64(column) => column.values.as_ptr().cast(),
                _ => unreachable!(),
            };
            let batch = ChunkedBatch {
                schema: first.schema.clone(),
                chunks: vec![first, second],
            };
            let descriptors = column_buffers(py, &batch, 0).unwrap().unwrap();
            let owner = descriptors[0].values.bind(py);
            assert_eq!(owner.get().data().unwrap().ptr, expected_ptr);
            let view = PyMemoryView::from(owner).unwrap();
            let slice = view.get_item(PySlice::new(py, 8, 16, 1)).unwrap();
            drop(view);
            drop(descriptors);
            drop(batch);
            assert!(first_weak.upgrade().is_some());
            assert!(second_weak.upgrade().is_none());
            let bytes: Vec<u8> = slice.call_method0("tobytes").unwrap().extract().unwrap();
            assert_eq!(bytes, 79_i64.to_ne_bytes());
            drop(slice);
            assert!(first_weak.upgrade().is_none());
        });
    }

    #[test]
    fn malformed_chunk_is_an_error_not_unsupported() {
        Python::initialize();
        Python::attach(|py| {
            let first = chunk(vec![13]);
            let mut broken = (*first).clone();
            broken.columns.clear();
            let batch = ChunkedBatch {
                schema: first.schema.clone(),
                chunks: vec![first, Arc::new(broken)],
            };
            // The first descriptor has already been allocated when the later
            // chunk fails. The error path must drop its owning references.
            let initial_refs = Arc::strong_count(&batch.chunks[0]);
            assert!(column_buffers(py, &batch, 0).is_err());
            assert_eq!(Arc::strong_count(&batch.chunks[0]), initial_refs);
        });
    }
}
