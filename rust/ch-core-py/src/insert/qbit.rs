use super::*;

fn qbit_value_count(name: &str, row_count: usize, dimension: usize) -> PyResult<usize> {
    row_count.checked_mul(dimension).ok_or_else(|| {
        PyValueError::new_err(format!(
            "column {name:?} QBit value count exceeds usize capacity"
        ))
    })
}

fn reserve_qbit_values<T>(name: &str, value_count: usize) -> PyResult<Vec<T>> {
    let mut values = Vec::new();
    values.try_reserve_exact(value_count).map_err(|_| {
        PyMemoryError::new_err(format!(
            "column {name:?} QBit value buffer cannot hold {value_count} elements"
        ))
    })?;
    Ok(values)
}

fn reserve_qbit_null_map(name: &str, row_count: usize) -> PyResult<Vec<u8>> {
    let mut nulls = Vec::new();
    nulls.try_reserve_exact(row_count).map_err(|_| {
        PyMemoryError::new_err(format!(
            "column {name:?} QBit null map cannot hold {row_count} rows"
        ))
    })?;
    Ok(nulls)
}

fn qbit_dimension_error(name: &str, row: usize, dimension: usize, actual: usize) -> PyErr {
    PyValueError::new_err(format!(
        "column {name:?} row {row} QBit dimension mismatch: expected {dimension}, got {actual}"
    ))
}

fn qbit_vector_error(name: &str, row: usize) -> PyErr {
    PyValueError::new_err(format!("column {name:?} row {row} is not a QBit vector"))
}

fn qbit_element_error(name: &str, row: usize, element: usize, type_name: &str) -> PyErr {
    PyValueError::new_err(format!(
        "column {name:?} row {row} element {element} cannot be converted to {type_name}"
    ))
}

fn map_buffer<T, U, F>(py: Python<'_>, name: &str, buffer: &PyBuffer<T>, map: F) -> PyResult<Vec<U>>
where
    T: Element + Copy,
    F: FnMut(usize, T) -> PyResult<U>,
{
    let mut out = reserve_qbit_values(name, buffer.item_count())?;
    extend_buffer(py, buffer, &mut out, map)?;
    Ok(out)
}

fn extend_buffer<T, U, F>(
    py: Python<'_>,
    buffer: &PyBuffer<T>,
    out: &mut Vec<U>,
    mut map: F,
) -> PyResult<()>
where
    T: Element + Copy,
    F: FnMut(usize, T) -> PyResult<U>,
{
    if let Some(values) = buffer.as_slice(py) {
        for (index, value) in values.iter().enumerate() {
            out.push(map(index, value.get())?);
        }
        return Ok(());
    }
    for (index, value) in buffer.to_vec(py)?.into_iter().enumerate() {
        out.push(map(index, value)?);
    }
    Ok(())
}

trait QBitValue: Copy {
    const DEFAULT: Self;
    const TYPE_NAME: &'static str;

    /// Fast exact-float/int conversion that cannot execute Python code.
    ///
    /// # Safety
    ///
    /// Requires the GIL and a valid, non-null object pointer.
    unsafe fn from_exact(ptr: *mut ffi::PyObject) -> Result<Self, ()>;

    fn from_object(value: &Bound<'_, PyAny>) -> Result<Self, ()>;

    fn matrix_buffer(
        py: Python<'_>,
        name: &str,
        value: &Bound<'_, PyAny>,
        row_count: usize,
        dimension: usize,
    ) -> PyResult<Option<Vec<Self>>>;

    fn append_vector_buffer(
        py: Python<'_>,
        name: &str,
        row: usize,
        value: &Bound<'_, PyAny>,
        dimension: usize,
        out: &mut Vec<Self>,
    ) -> PyResult<bool>;

    fn into_child(values: Vec<Self>) -> Column;
}

unsafe fn exact_f64(ptr: *mut ffi::PyObject) -> Result<f64, ()> {
    <f64 as FastValue>::from_exact(ptr, &ChType::Float64, 0)
}

impl QBitValue for f32 {
    const DEFAULT: Self = 0.0;
    const TYPE_NAME: &'static str = "Float32";

    unsafe fn from_exact(ptr: *mut ffi::PyObject) -> Result<Self, ()> {
        exact_f64(ptr).map(|value| value as f32)
    }

    fn from_object(value: &Bound<'_, PyAny>) -> Result<Self, ()> {
        value
            .extract::<f64>()
            .map(|value| value as f32)
            .map_err(|_| ())
    }

    fn matrix_buffer(
        py: Python<'_>,
        name: &str,
        value: &Bound<'_, PyAny>,
        row_count: usize,
        dimension: usize,
    ) -> PyResult<Option<Vec<Self>>> {
        let shape = [row_count, dimension];
        if let Some(buffer) = matching_native_buffer::<f32>(value, &shape) {
            // Keep PyO3's single-allocation PyBuffer_ToContiguous path here.
            // The shape is already exact, and avoiding a zero-initialization
            // pass preserves memcpy-level throughput for large matrices.
            return buffer.to_vec(py).map(Some);
        }
        matching_native_buffer::<f64>(value, &shape)
            .map(|buffer| map_buffer(py, name, &buffer, |_index, value| Ok(value as f32)))
            .transpose()
    }

    fn append_vector_buffer(
        py: Python<'_>,
        _name: &str,
        _row: usize,
        value: &Bound<'_, PyAny>,
        dimension: usize,
        out: &mut Vec<Self>,
    ) -> PyResult<bool> {
        let shape = [dimension];
        if let Some(buffer) = matching_native_buffer::<f32>(value, &shape) {
            extend_buffer(py, &buffer, out, |_index, value| Ok(value))?;
            return Ok(true);
        }
        if let Some(buffer) = matching_native_buffer::<f64>(value, &shape) {
            extend_buffer(py, &buffer, out, |_index, value| Ok(value as f32))?;
            return Ok(true);
        }
        Ok(false)
    }

    fn into_child(values: Vec<Self>) -> Column {
        Column::Float32(PrimitiveColumn::new(values))
    }
}

impl QBitValue for f64 {
    const DEFAULT: Self = 0.0;
    const TYPE_NAME: &'static str = "Float64";

    unsafe fn from_exact(ptr: *mut ffi::PyObject) -> Result<Self, ()> {
        exact_f64(ptr)
    }

    fn from_object(value: &Bound<'_, PyAny>) -> Result<Self, ()> {
        value.extract::<f64>().map_err(|_| ())
    }

    fn matrix_buffer(
        py: Python<'_>,
        name: &str,
        value: &Bound<'_, PyAny>,
        row_count: usize,
        dimension: usize,
    ) -> PyResult<Option<Vec<Self>>> {
        let shape = [row_count, dimension];
        if let Some(buffer) = matching_native_buffer::<f64>(value, &shape) {
            // See the Float32 path above. PyO3 owns the exact-sized matrix
            // allocation so the matching-dtype case stays a contiguous copy.
            return buffer.to_vec(py).map(Some);
        }
        matching_native_buffer::<f32>(value, &shape)
            .map(|buffer| map_buffer(py, name, &buffer, |_index, value| Ok(value.into())))
            .transpose()
    }

    fn append_vector_buffer(
        py: Python<'_>,
        _name: &str,
        _row: usize,
        value: &Bound<'_, PyAny>,
        dimension: usize,
        out: &mut Vec<Self>,
    ) -> PyResult<bool> {
        let shape = [dimension];
        if let Some(buffer) = matching_native_buffer::<f64>(value, &shape) {
            extend_buffer(py, &buffer, out, |_index, value| Ok(value))?;
            return Ok(true);
        }
        if let Some(buffer) = matching_native_buffer::<f32>(value, &shape) {
            extend_buffer(py, &buffer, out, |_index, value| Ok(value.into()))?;
            return Ok(true);
        }
        Ok(false)
    }

    fn into_child(values: Vec<Self>) -> Column {
        Column::Float64(PrimitiveColumn::new(values))
    }
}

impl QBitValue for [u8; 2] {
    const DEFAULT: Self = [0; 2];
    const TYPE_NAME: &'static str = "BFloat16";

    unsafe fn from_exact(ptr: *mut ffi::PyObject) -> Result<Self, ()> {
        exact_f64(ptr).and_then(checked_f64_to_bfloat16)
    }

    fn from_object(value: &Bound<'_, PyAny>) -> Result<Self, ()> {
        value
            .extract::<f64>()
            .map_err(|_| ())
            .and_then(checked_f64_to_bfloat16)
    }

    fn matrix_buffer(
        py: Python<'_>,
        name: &str,
        value: &Bound<'_, PyAny>,
        row_count: usize,
        dimension: usize,
    ) -> PyResult<Option<Vec<Self>>> {
        let shape = [row_count, dimension];
        let convert = |index: usize, value: f64| {
            checked_f64_to_bfloat16(value).map_err(|_| {
                qbit_element_error(name, index / dimension, index % dimension, Self::TYPE_NAME)
            })
        };
        if let Some(buffer) = matching_native_buffer::<f32>(value, &shape) {
            return map_buffer(py, name, &buffer, |index, value| {
                convert(index, value.into())
            })
            .map(Some);
        }
        matching_native_buffer::<f64>(value, &shape)
            .map(|buffer| map_buffer(py, name, &buffer, convert))
            .transpose()
    }

    fn append_vector_buffer(
        py: Python<'_>,
        name: &str,
        row: usize,
        value: &Bound<'_, PyAny>,
        dimension: usize,
        out: &mut Vec<Self>,
    ) -> PyResult<bool> {
        let shape = [dimension];
        let convert = |element: usize, value: f64| {
            checked_f64_to_bfloat16(value)
                .map_err(|_| qbit_element_error(name, row, element, Self::TYPE_NAME))
        };
        if let Some(buffer) = matching_native_buffer::<f32>(value, &shape) {
            extend_buffer(py, &buffer, out, |element, value| {
                convert(element, value.into())
            })?;
            return Ok(true);
        }
        if let Some(buffer) = matching_native_buffer::<f64>(value, &shape) {
            extend_buffer(py, &buffer, out, convert)?;
            return Ok(true);
        }
        Ok(false)
    }

    fn into_child(values: Vec<Self>) -> Column {
        Column::BFloat16(PrimitiveColumn::new(values))
    }
}

fn append_qbit_seq<T: QBitValue, S: FastSeq>(
    py: Python<'_>,
    name: &str,
    row: usize,
    seq: &S,
    dimension: usize,
    out: &mut Vec<T>,
) -> PyResult<()> {
    let actual = seq.size();
    if actual != dimension {
        return Err(qbit_dimension_error(name, row, dimension, actual));
    }
    for element in 0..dimension {
        // The sequence length is checked above and after every fallback that
        // may execute Python code. The temporary strong reference must drop
        // before that check because its finalizer can also resize `seq`.
        let ptr = unsafe { seq.get(element) };
        match unsafe { T::from_exact(ptr) } {
            Ok(value) => out.push(value),
            Err(()) => {
                let value = unsafe { Bound::from_borrowed_ptr(py, ptr) };
                let converted = T::from_object(&value)
                    .map_err(|_| qbit_element_error(name, row, element, T::TYPE_NAME))?;
                out.push(converted);
                drop(value);
                check_not_resized(seq, name, dimension)?;
            }
        }
    }
    Ok(())
}

fn append_qbit_vector<T: QBitValue>(
    py: Python<'_>,
    name: &str,
    row: usize,
    value: &Bound<'_, PyAny>,
    dimension: usize,
    out: &mut Vec<T>,
) -> PyResult<()> {
    if let Ok(list) = value.downcast_exact::<PyList>() {
        return append_qbit_seq(py, name, row, &ListSeq(list), dimension, out);
    }
    if let Ok(tuple) = value.downcast_exact::<PyTuple>() {
        return append_qbit_seq(py, name, row, &TupleSeq(tuple), dimension, out);
    }
    if T::append_vector_buffer(py, name, row, value, dimension, out)? {
        return Ok(());
    }
    if is_string_or_bytes_like(value) {
        return Err(qbit_vector_error(name, row));
    }
    let actual = value.len().map_err(|_| qbit_vector_error(name, row))?;
    if actual != dimension {
        return Err(qbit_dimension_error(name, row, dimension, actual));
    }
    for element in 0..dimension {
        let item = value
            .get_item(element)
            .map_err(|_| qbit_vector_error(name, row))?;
        out.push(
            T::from_object(&item)
                .map_err(|_| qbit_element_error(name, row, element, T::TYPE_NAME))?,
        );
    }
    Ok(())
}

fn finish_qbit<T: QBitValue>(values: Vec<T>, dimension: usize, validity: Option<Bitmap>) -> Column {
    let values = T::into_child(values);
    Column::QBit(match validity {
        Some(validity) => QBitColumn::new_nullable(values, dimension, validity),
        None => QBitColumn::new(values, dimension),
    })
}

fn qbit_column_from_seq<T: QBitValue, S: FastSeq>(
    py: Python<'_>,
    name: &str,
    qbit_type: &ChType,
    dimension: usize,
    seq: &S,
    row_count: usize,
    nullable: bool,
) -> PyResult<Column> {
    if seq.size() != row_count {
        return Err(PyValueError::new_err(format!(
            "column {name:?} has {} values but row_count is {row_count}",
            seq.size()
        )));
    }
    let value_count = qbit_value_count(name, row_count, dimension)?;
    let mut values = reserve_qbit_values(name, value_count)?;
    let mut null_map = if nullable {
        Some(reserve_qbit_null_map(name, row_count)?)
    } else {
        None
    };
    for row in 0..row_count {
        let ptr = unsafe { seq.get(row) };
        if ptr == unsafe { ffi::Py_None() } {
            let Some(nulls) = &mut null_map else {
                return Err(PyValueError::new_err(format!(
                    "column {name:?} row {row} is None but {qbit_type} is not Nullable"
                )));
            };
            nulls.push(1);
            values.resize(values.len() + dimension, T::DEFAULT);
            continue;
        }
        if let Some(nulls) = &mut null_map {
            nulls.push(0);
        }
        let value = unsafe { Bound::from_borrowed_ptr(py, ptr) };
        append_qbit_vector(py, name, row, &value, dimension, &mut values)?;
        // Dropping the row can run a finalizer that resizes `seq`, so it must
        // happen before the next unchecked list read is declared safe.
        drop(value);
        check_not_resized(seq, name, row_count)?;
    }
    let validity = null_map.map(|nulls| Bitmap::from_ch_null_map(&nulls));
    Ok(finish_qbit(values, dimension, validity))
}

fn qbit_column_from_rows<'py, T: QBitValue, R: RowAccess<'py>>(
    py: Python<'py>,
    name: &str,
    qbit_type: &ChType,
    dimension: usize,
    rows: &R,
    row_count: usize,
    nullable: bool,
) -> PyResult<Column> {
    let value_count = qbit_value_count(name, row_count, dimension)?;
    let mut values = reserve_qbit_values(name, value_count)?;
    let mut null_map = if nullable {
        Some(reserve_qbit_null_map(name, row_count)?)
    } else {
        None
    };
    for row in 0..row_count {
        let value = rows.value(row)?;
        if value.is_none() {
            let Some(nulls) = &mut null_map else {
                return Err(PyValueError::new_err(format!(
                    "column {name:?} row {row} is None but {qbit_type} is not Nullable"
                )));
            };
            nulls.push(1);
            values.resize(values.len() + dimension, T::DEFAULT);
            continue;
        }
        if let Some(nulls) = &mut null_map {
            nulls.push(0);
        }
        append_qbit_vector(py, name, row, &value, dimension, &mut values)?;
        drop(value);
        rows.validate()?;
    }
    let validity = null_map.map(|nulls| Bitmap::from_ch_null_map(&nulls));
    Ok(finish_qbit(values, dimension, validity))
}

fn build_typed_qbit<T: QBitValue>(
    py: Python<'_>,
    name: &str,
    qbit_type: &ChType,
    dimension: usize,
    values: &Bound<'_, PyAny>,
    row_count: usize,
    nullable: bool,
) -> PyResult<Column> {
    if let Some(matrix) = T::matrix_buffer(py, name, values, row_count, dimension)? {
        let validity = nullable.then(|| Bitmap::all_valid(row_count));
        return Ok(finish_qbit(matrix, dimension, validity));
    }
    if let Ok(list) = values.downcast_exact::<PyList>() {
        return qbit_column_from_seq::<T, _>(
            py,
            name,
            qbit_type,
            dimension,
            &ListSeq(list),
            row_count,
            nullable,
        );
    }
    if let Ok(tuple) = values.downcast_exact::<PyTuple>() {
        return qbit_column_from_seq::<T, _>(
            py,
            name,
            qbit_type,
            dimension,
            &TupleSeq(tuple),
            row_count,
            nullable,
        );
    }
    let rows = ColumnValues::new(values, name)?;
    check_row_count(name, &rows, row_count)?;
    qbit_column_from_rows::<T, _>(py, name, qbit_type, dimension, &rows, row_count, nullable)
}

pub(super) fn build_qbit_column(
    py: Python<'_>,
    name: &str,
    element_type: QBitElementType,
    dimension: usize,
    values: &Bound<'_, PyAny>,
    row_count: usize,
    nullable: bool,
) -> PyResult<Column> {
    let qbit_type = ChType::QBit {
        element_type,
        dimension,
    };
    match element_type {
        QBitElementType::BFloat16 => build_typed_qbit::<[u8; 2]>(
            py, name, &qbit_type, dimension, values, row_count, nullable,
        ),
        QBitElementType::Float32 => {
            build_typed_qbit::<f32>(py, name, &qbit_type, dimension, values, row_count, nullable)
        }
        QBitElementType::Float64 => {
            build_typed_qbit::<f64>(py, name, &qbit_type, dimension, values, row_count, nullable)
        }
    }
}

pub(super) fn build_qbit_column_from_seq<S: FastSeq>(
    py: Python<'_>,
    name: &str,
    element_type: QBitElementType,
    dimension: usize,
    seq: &S,
    row_count: usize,
    nullable: bool,
) -> PyResult<Column> {
    let qbit_type = ChType::QBit {
        element_type,
        dimension,
    };
    match element_type {
        QBitElementType::BFloat16 => qbit_column_from_seq::<[u8; 2], _>(
            py, name, &qbit_type, dimension, seq, row_count, nullable,
        ),
        QBitElementType::Float32 => qbit_column_from_seq::<f32, _>(
            py, name, &qbit_type, dimension, seq, row_count, nullable,
        ),
        QBitElementType::Float64 => qbit_column_from_seq::<f64, _>(
            py, name, &qbit_type, dimension, seq, row_count, nullable,
        ),
    }
}
