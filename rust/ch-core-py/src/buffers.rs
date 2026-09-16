//! Private, read-only buffer exports over immutable decoded chunks.

use std::ffi::{c_int, c_void};
use std::sync::Arc;

use ch_core_rs::batch::{ChunkedBatch, ColBatch};
use ch_core_rs::bitmap::Bitmap;
use ch_core_rs::column::{ArrayColumn, Column, PrimitiveColumn};
use ch_core_rs::schema::ChType;
use pyo3::exceptions::{PyBufferError, PyValueError};
use pyo3::ffi;
use pyo3::prelude::*;

/// Physical metadata stays separate from the ClickHouse logical schema.
#[pyclass(name = "_ColumnBuffers", frozen, get_all)]
pub(crate) struct ColumnBuffers {
    kind: &'static str,
    // Bool and Array report 0.
    itemsize: usize,
    byteorder: &'static str,
    length: usize,
    null_count: usize,
    values: Option<Py<Buffer>>,
    validity: Option<Py<Buffer>>,
    offsets: Option<Py<Buffer>>,
    child: Option<Py<ColumnBuffers>>,
}

#[derive(Clone, Copy)]
enum Selection {
    Values,
    Validity,
    Offsets,
}

/// No pointers or Python references are stored here. An exported view pins
/// exactly one immutable chunk, including its other columns.
#[pyclass(name = "_Buffer", frozen, weakref)]
struct Buffer {
    chunk: Arc<ColBatch>,
    index: usize,
    array_depth: usize,
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
        let mut column = self
            .chunk
            .columns
            .get(self.index)
            .ok_or("Missing buffer column")?;
        for _ in 0..self.array_depth {
            let Column::Array(array) = column else {
                return Err("Missing array buffer child");
            };
            column = &array.values;
        }
        match self.selection {
            Selection::Offsets => {
                let Column::Array(array) = column else {
                    return Err("Missing array offsets");
                };
                offset_buffer(array)
            }
            Selection::Values => {
                scalar_column(column)
                    .ok_or("Unsupported buffer storage")?
                    .values
            }
            Selection::Validity => {
                let scalar = scalar_column(column).ok_or("Unsupported buffer storage")?;
                let bitmap = scalar.validity.ok_or("Missing validity buffer")?;
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
    bitmap_buffer(bitmap.as_bytes(), bitmap.len())
}

fn bitmap_buffer(bytes: &[u8], length: usize) -> Result<BufferData, &'static str> {
    let bytes = bytes
        .get(..length.div_ceil(8))
        .ok_or("Bitmap buffer storage is shorter than its logical length")?;
    BufferData::new(bytes.as_ptr().cast(), bytes.len(), 1)
}

const NATIVE_BYTEORDER: &str = if cfg!(target_endian = "little") {
    "little"
} else {
    "big"
};

struct ScalarColumn<'a> {
    kind: &'static str,
    itemsize: usize,
    byteorder: &'static str,
    length: usize,
    values: Result<BufferData, &'static str>,
    validity: Option<&'a Bitmap>,
}

fn primitive<'a, T: Clone>(
    column: &'a PrimitiveColumn<T>,
    kind: &'static str,
    byteorder: &'static str,
) -> ScalarColumn<'a> {
    let values = column.values.as_slice();
    let itemsize = std::mem::size_of::<T>();
    ScalarColumn {
        kind,
        itemsize,
        byteorder,
        length: values.len(),
        values: BufferData::new(values.as_ptr().cast(), values.len(), itemsize),
        validity: column.validity.as_ref(),
    }
}

fn scalar_column(column: &Column) -> Option<ScalarColumn<'_>> {
    let host = NATIVE_BYTEORDER;
    Some(match column {
        Column::Int8(c) => primitive(c, "int8", host),
        Column::Int16(c) => primitive(c, "int16", host),
        Column::Int32(c) | Column::Date32(c) | Column::Time(c) => primitive(c, "int32", host),
        Column::Int64(c) | Column::DateTime64(c) | Column::Time64(c) | Column::Interval(c) => {
            primitive(c, "int64", host)
        }
        Column::UInt8(c) => primitive(c, "uint8", host),
        Column::UInt16(c) | Column::Date(c) => primitive(c, "uint16", host),
        Column::UInt32(c) | Column::DateTime(c) => primitive(c, "uint32", host),
        Column::UInt64(c) => primitive(c, "uint64", host),
        Column::Float32(c) => primitive(c, "float32", host),
        Column::Float64(c) => primitive(c, "float64", host),
        Column::BFloat16(c) => primitive(c, "bfloat16", "little"),
        Column::Bool(c) => ScalarColumn {
            kind: "bool_bitmap",
            itemsize: 0,
            byteorder: "not-applicable",
            length: c.len,
            values: bitmap_buffer(&c.bitmap, c.len),
            validity: c.validity.as_ref(),
        },
        _ => return None,
    })
}

fn scalar_kind(ch_type: &ChType) -> Option<&'static str> {
    Some(match ch_type {
        ChType::Int8 => "int8",
        ChType::Int16 => "int16",
        ChType::Int32 | ChType::Date32 | ChType::Time => "int32",
        ChType::Int64 | ChType::DateTime64 { .. } | ChType::Time64 { .. } | ChType::Interval(_) => {
            "int64"
        }
        ChType::UInt8 => "uint8",
        ChType::UInt16 | ChType::Date => "uint16",
        ChType::UInt32 | ChType::DateTime { .. } => "uint32",
        ChType::UInt64 => "uint64",
        ChType::Float32 => "float32",
        ChType::Float64 => "float64",
        ChType::BFloat16 => "bfloat16",
        ChType::Bool => "bool_bitmap",
        ChType::Nullable(inner) | ChType::SimpleAggregateFunction { inner, .. } => {
            return scalar_kind(inner);
        }
        _ => return None,
    })
}

fn validate_scalar(column: &ScalarColumn<'_>, kind: &str, rows: usize) -> Result<(), &'static str> {
    if column.kind != kind {
        return Err("Scalar buffer storage differs from the schema");
    }
    if column.length != rows {
        return Err("Scalar buffer length differs from the chunk row count");
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

struct BufferLayout {
    array_depth: usize,
    leaf_kind: &'static str,
}

fn buffer_layout(mut ch_type: &ChType) -> Option<BufferLayout> {
    if let Some(leaf_kind) = scalar_kind(ch_type) {
        return Some(BufferLayout {
            array_depth: 0,
            leaf_kind,
        });
    }
    let mut array_depth = 0_usize;
    let mut nullable_leaf = false;
    loop {
        match ch_type {
            ChType::SimpleAggregateFunction { inner, .. } => ch_type = inner,
            ChType::Array(inner) if !nullable_leaf => {
                array_depth = array_depth.checked_add(1)?;
                ch_type = inner;
            }
            ChType::Nullable(inner) if array_depth > 0 && !nullable_leaf => {
                nullable_leaf = true;
                ch_type = inner;
            }
            ChType::Time if array_depth > 0 => {
                return Some(BufferLayout {
                    array_depth,
                    leaf_kind: "int32",
                });
            }
            ChType::Time64 { .. } if array_depth > 0 => {
                return Some(BufferLayout {
                    array_depth,
                    leaf_kind: "int64",
                });
            }
            _ => return None,
        }
    }
}

fn offset_buffer(array: &ArrayColumn) -> Result<BufferData, &'static str> {
    BufferData::new(
        array.offsets.as_ptr().cast(),
        array.offsets.len(),
        std::mem::size_of::<i64>(),
    )
}

fn validate_offsets(
    array: &ArrayColumn,
    rows: usize,
    child_length: usize,
) -> Result<(), &'static str> {
    let count = rows
        .checked_add(1)
        .ok_or("Array offset count overflows usize")?;
    if array.offsets.len() != count {
        return Err("Array offset count differs from the row count plus one");
    }
    if array.offsets.first() != Some(&0) {
        return Err("Array offsets must start at zero");
    }
    let mut previous = 0;
    for &offset in &array.offsets {
        let offset = usize::try_from(offset).map_err(|_| "Array offset is outside usize bounds")?;
        if offset < previous || offset > child_length {
            return Err("Array offsets must be ordered and within the child length");
        }
        previous = offset;
    }
    if previous != child_length {
        return Err("Final array offset differs from the child length");
    }
    offset_buffer(array)?;
    Ok(())
}

fn column_descriptor(
    py: Python<'_>,
    chunk: &Arc<ColBatch>,
    index: usize,
    column: &Column,
    array_depth: usize,
    layout: &BufferLayout,
) -> PyResult<ColumnBuffers> {
    let owner = |selection| {
        Py::new(
            py,
            Buffer {
                chunk: Arc::clone(chunk),
                index,
                array_depth,
                selection,
            },
        )
    };
    if array_depth < layout.array_depth {
        let Column::Array(array) = column else {
            return Err(PyValueError::new_err(
                "Array buffer storage differs from the schema",
            ));
        };
        let length = if array_depth == 0 {
            chunk.num_rows
        } else {
            array
                .offsets
                .len()
                .checked_sub(1)
                .ok_or_else(|| PyValueError::new_err("Missing array offsets"))?
        };
        let child = column_descriptor(py, chunk, index, &array.values, array_depth + 1, layout)?;
        validate_offsets(array, length, child.length).map_err(PyValueError::new_err)?;
        return Ok(ColumnBuffers {
            kind: "array",
            itemsize: 0,
            byteorder: "not-applicable",
            length,
            null_count: 0,
            values: None,
            validity: None,
            offsets: Some(owner(Selection::Offsets)?),
            child: Some(Py::new(py, child)?),
        });
    }
    let scalar = scalar_column(column)
        .ok_or_else(|| PyValueError::new_err("Scalar buffer storage differs from the schema"))?;
    let rows = if array_depth == 0 {
        chunk.num_rows
    } else {
        scalar.length
    };
    validate_scalar(&scalar, layout.leaf_kind, rows).map_err(PyValueError::new_err)?;
    Ok(ColumnBuffers {
        kind: layout.leaf_kind,
        itemsize: scalar.itemsize,
        byteorder: scalar.byteorder,
        length: scalar.length,
        null_count: scalar.validity.map_or(0, Bitmap::null_count),
        values: Some(owner(Selection::Values)?),
        validity: scalar
            .validity
            .map(|_| owner(Selection::Validity))
            .transpose()?,
        offsets: None,
        child: None,
    })
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
    let Some(layout) = buffer_layout(&field.ch_type) else {
        return Ok(None);
    };
    let mut descriptors = Vec::with_capacity(batch.chunks.len());
    for chunk in &batch.chunks {
        if chunk.columns.len() != batch.num_columns() {
            return Err(PyValueError::new_err(
                "Chunk column count differs from the schema",
            ));
        }
        descriptors.push(column_descriptor(
            py,
            chunk,
            index,
            &chunk.columns[index],
            0,
            &layout,
        )?);
    }
    Ok(Some(descriptors))
}

#[cfg(test)]
mod tests {
    use super::*;
    use ch_core_rs::column::BoolColumn;
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

    fn temporal_array_chunk(outer_offsets: Vec<i64>, inner_offsets: Vec<i64>) -> Arc<ColBatch> {
        Arc::new(ColBatch {
            schema: Schema::new(vec![Field {
                name: "v".into(),
                ch_type: ChType::Array(Box::new(ChType::Array(Box::new(ChType::Nullable(
                    Box::new(ChType::Time64 { precision: 9 }),
                ))))),
            }]),
            columns: vec![Column::Array(ArrayColumn::new(
                outer_offsets,
                Column::Array(ArrayColumn::new(
                    inner_offsets,
                    Column::Time64(PrimitiveColumn::new_nullable(
                        vec![13, 0, 79],
                        Bitmap::from_ch_null_map(&[0, 1, 0]),
                    )),
                )),
            ))],
            num_rows: 2,
        })
    }

    #[test]
    fn validates_array_offsets_before_export() {
        for (offsets, rows, child_length, message) in [
            (vec![], 0, 0, "count differs"),
            (vec![0, 1], 0, 1, "count differs"),
            (vec![1, 1], 1, 1, "start at zero"),
            (vec![0, -1], 1, 0, "outside usize"),
            (vec![0, 2, 1], 2, 2, "ordered and within"),
            (vec![0, 1], 1, 2, "Final array offset"),
            (vec![0, 3], 1, 2, "ordered and within"),
            (vec![0], usize::MAX, 0, "overflows usize"),
        ] {
            let column = ArrayColumn::new(offsets, Column::Time(PrimitiveColumn::new(vec![])));
            let error = validate_offsets(&column, rows, child_length).unwrap_err();
            assert!(error.contains(message), "{error}");
        }
        for (offsets, rows, child_length) in [(vec![0], 0, 0), (vec![0, 0, 2, 2], 3, 2)] {
            let column = ArrayColumn::new(offsets, Column::Time(PrimitiveColumn::new(vec![])));
            assert!(validate_offsets(&column, rows, child_length).is_ok());
        }
        let time = ChType::Time;
        assert!(
            buffer_layout(&ChType::Nullable(Box::new(ChType::Array(Box::new(
                time.clone()
            )))))
            .is_none()
        );
        assert!(
            buffer_layout(&ChType::Array(Box::new(ChType::Nullable(Box::new(
                ChType::Array(Box::new(time))
            )))))
            .is_none()
        );
    }

    #[test]
    fn every_array_selection_pins_only_source_chunk_until_final_release() {
        Python::initialize();
        Python::attach(|py| {
            for (array_depth, selection) in [
                (0, Selection::Offsets),
                (1, Selection::Offsets),
                (2, Selection::Values),
                (2, Selection::Validity),
            ] {
                let first = temporal_array_chunk(vec![0, 1, 2], vec![0, 2, 3]);
                let other = temporal_array_chunk(vec![0, 0, 2], vec![0, 1, 3]);
                let first_weak = Arc::downgrade(&first);
                let other_weak = Arc::downgrade(&other);
                let mut source = &first.columns[0];
                for _ in 0..array_depth {
                    let Column::Array(array) = source else {
                        unreachable!()
                    };
                    source = &array.values;
                }
                let expected_ptr = match (source, selection) {
                    (Column::Array(array), Selection::Offsets) => array.offsets.as_ptr().cast(),
                    (Column::Time64(column), Selection::Values) => column.values.as_ptr().cast(),
                    (Column::Time64(column), Selection::Validity) => {
                        column.validity.as_ref().unwrap().as_bytes().as_ptr().cast()
                    }
                    _ => unreachable!(),
                };
                let batch = ChunkedBatch {
                    schema: first.schema.clone(),
                    chunks: vec![first, other],
                };
                let descriptors = column_buffers(py, &batch, 0).unwrap().unwrap();
                let mut descriptor = &descriptors[0];
                for _ in 0..array_depth {
                    descriptor = descriptor.child.as_ref().unwrap().bind(py).get();
                }
                let owner = match selection {
                    Selection::Offsets => &descriptor.offsets,
                    Selection::Values => &descriptor.values,
                    Selection::Validity => &descriptor.validity,
                }
                .as_ref()
                .unwrap()
                .bind(py);
                assert_eq!(owner.get().data().unwrap().ptr, expected_ptr);
                let view = PyMemoryView::from(owner).unwrap();
                let expected: Vec<u8> = view.call_method0("tobytes").unwrap().extract().unwrap();
                let slice = view.get_item(PySlice::new(py, 0, 1, 1)).unwrap();
                drop(view);
                drop(descriptors);
                drop(batch);
                assert!(first_weak.upgrade().is_some());
                assert!(other_weak.upgrade().is_none());
                let actual: Vec<u8> = slice.call_method0("tobytes").unwrap().extract().unwrap();
                assert_eq!(actual, expected[..1]);
                drop(slice);
                assert!(first_weak.upgrade().is_none());
            }
        });
    }

    #[test]
    fn malformed_array_releases_previously_allocated_descriptors() {
        Python::initialize();
        Python::attach(|py| {
            // Every layout except the empty inner offsets fails after leaf owners exist.
            let mut malformed: Vec<_> = [
                (vec![0, 1, 1], vec![0, 2, 3]),
                (vec![0, 0, 0], vec![]),
                (vec![0, 0, 0], vec![0]),
                (vec![0, 1, 2], vec![0, 1, 2]),
            ]
            .into_iter()
            .map(|(outer, inner)| temporal_array_chunk(outer, inner))
            .collect();
            for leaf in [
                Column::Time(PrimitiveColumn::new(vec![13, 0, 79])),
                Column::Time64(PrimitiveColumn::new_nullable(
                    vec![13, 0, 79],
                    Bitmap::all_valid(2),
                )),
            ] {
                let mut broken = temporal_array_chunk(vec![0, 1, 2], vec![0, 2, 3]);
                let mut column = &mut Arc::get_mut(&mut broken).unwrap().columns[0];
                for _ in 0..2 {
                    let Column::Array(array) = column else {
                        unreachable!()
                    };
                    column = &mut array.values;
                }
                *column = leaf;
                malformed.push(broken);
            }
            for broken in malformed {
                let first = temporal_array_chunk(vec![0, 1, 2], vec![0, 2, 3]);
                let first_weak = Arc::downgrade(&first);
                let broken_weak = Arc::downgrade(&broken);
                let batch = ChunkedBatch {
                    schema: first.schema.clone(),
                    chunks: vec![first, broken],
                };
                assert!(column_buffers(py, &batch, 0).is_err());
                assert_eq!(Arc::strong_count(&batch.chunks[0]), 1);
                assert_eq!(Arc::strong_count(&batch.chunks[1]), 1);
                drop(batch);
                assert!(first_weak.upgrade().is_none());
                assert!(broken_weak.upgrade().is_none());
            }
        });
    }

    #[test]
    fn validates_numeric_metadata_and_lengths() {
        let column = Column::Int64(PrimitiveColumn::new(vec![13, 79]));
        let numeric = scalar_column(&column).unwrap();
        assert!(validate_scalar(&numeric, "int64", 2).is_ok());
        assert!(validate_scalar(&numeric, "uint64", 2).is_err());
        assert!(validate_scalar(&numeric, "int64", 3).is_err());
        let short_bitmap = Column::Int64(PrimitiveColumn::new_nullable(
            vec![13, 79],
            Bitmap::all_valid(1),
        ));
        assert!(validate_scalar(&scalar_column(&short_bitmap).unwrap(), "int64", 2).is_err());
        let ptr = std::ptr::NonNull::<u8>::dangling().as_ptr().cast();
        assert!(BufferData::new(ptr, usize::MAX, 8).is_err());
        assert!(BufferData::new(ptr, isize::MAX as usize + 1, 1).is_err());
    }

    #[test]
    fn validates_boolean_bitmap_storage_and_length() {
        for rows in [0_usize, 1, 7, 8, 9, 64, 65] {
            for nullable in [false, true] {
                let byte_length = rows.div_ceil(8);
                let column = BoolColumn {
                    bitmap: vec![0xa5; byte_length + 13],
                    len: rows,
                    validity: nullable.then(|| Bitmap::all_valid(rows)),
                };
                let wrapped = Column::Bool(column.clone());
                let scalar = scalar_column(&wrapped).unwrap();
                assert!(validate_scalar(&scalar, "bool_bitmap", rows).is_ok());
                assert!(validate_scalar(&scalar, "uint8", rows).is_err());
                assert!(validate_scalar(&scalar, "bool_bitmap", rows + 1).is_err());
                let mut malformed = column.clone();
                malformed.validity = Some(Bitmap::all_valid(rows + 1));
                assert!(validate_scalar(
                    &scalar_column(&Column::Bool(malformed.clone())).unwrap(),
                    "bool_bitmap",
                    rows
                )
                .is_err());
                if rows > 0 {
                    malformed.validity = None;
                    malformed.bitmap.truncate(byte_length - 1);
                    assert!(validate_scalar(
                        &scalar_column(&Column::Bool(malformed)).unwrap(),
                        "bool_bitmap",
                        rows
                    )
                    .is_err());
                }
                let expected_ptr = column.bitmap.as_ptr().cast();
                let buffer = Buffer {
                    chunk: Arc::new(ColBatch::new(
                        Schema::new(vec![Field {
                            name: "v".into(),
                            ch_type: ChType::Bool,
                        }]),
                        vec![Column::Bool(column)],
                        rows,
                    )),
                    index: 0,
                    array_depth: 0,
                    selection: Selection::Values,
                };
                let data = buffer.data().unwrap();
                assert_eq!(data.len as usize, byte_length);
                assert_eq!(data.ptr, expected_ptr);
            }
        }
    }

    #[test]
    fn bfloat16_exports_exact_words_without_copying() {
        let values = vec![[0x00, 0x80], [0xc1, 0x7f], [0x01, 0x00]];
        let expected_ptr = values.as_ptr().cast();
        let column = Column::BFloat16(PrimitiveColumn::new(values));
        let scalar = scalar_column(&column).unwrap();
        assert_eq!(scalar.itemsize, 2);
        assert_eq!(scalar.byteorder, "little");
        assert!(validate_scalar(&scalar, "bfloat16", 3).is_ok());
        assert!(validate_scalar(&scalar, "uint16", 3).is_err());
        assert!(validate_scalar(&scalar, "bfloat16", 2).is_err());
        let data = scalar.values.unwrap();
        assert_eq!(data.ptr, expected_ptr);
        assert_eq!(data.len, 6);
    }

    #[test]
    fn validity_export_uses_logical_byte_length() {
        for rows in [0_usize, 7, 8, 9, 64, 65] {
            let byte_length = rows.div_ceil(8);
            let bitmap = Bitmap::from_raw(vec![0xff; byte_length + 13], rows);
            let expected_ptr = bitmap.as_bytes().as_ptr().cast();
            let column = Column::Int64(PrimitiveColumn::new_nullable(vec![13; rows], bitmap));
            assert!(validate_scalar(&scalar_column(&column).unwrap(), "int64", rows).is_ok());
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
                array_depth: 0,
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
        assert!(validate_scalar(&scalar_column(&column).unwrap(), "int64", 2).is_err());
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
                    array_depth: 0,
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
            let owner = descriptors[0].values.as_ref().unwrap().bind(py);
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
