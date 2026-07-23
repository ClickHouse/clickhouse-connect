use pyo3::prelude::*;

mod batch;
mod decoder;
mod insert;
mod pyval;

#[pymodule]
fn _ch_core(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    // Binding API contract number checked by clickhouse_connect/driver/rustcodec.py
    // at import; bump when the Python-visible binding surface changes incompatibly.
    m.add("BINDING_API_VERSION", 1)?;
    m.add_class::<batch::ColBatch>()?;
    m.add_class::<decoder::BlockDecoder>()?;
    m.add_class::<decoder::StreamDecoder>()?;
    #[cfg(unix)]
    m.add_class::<decoder::PipeDecoder>()?;
    m.add_function(wrap_pyfunction!(insert::encode_native_block, m)?)?;
    Ok(())
}
