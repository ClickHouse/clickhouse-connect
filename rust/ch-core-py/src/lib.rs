use pyo3::prelude::*;

mod batch;
mod decoder;
mod insert;
mod pyval;

#[pymodule]
fn _ch_core(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("__version__", "0.1.0")?;
    m.add_class::<batch::ColBatch>()?;
    m.add_class::<decoder::BlockDecoder>()?;
    m.add_class::<decoder::StreamDecoder>()?;
    #[cfg(unix)]
    m.add_class::<decoder::PipeDecoder>()?;
    m.add_function(wrap_pyfunction!(insert::encode_native_block, m)?)?;
    Ok(())
}
