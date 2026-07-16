use pyo3::buffer::PyBuffer;
use pyo3::exceptions::{
    PyEOFError, PyNotImplementedError, PyOSError, PyRuntimeError, PyStopIteration, PyValueError,
};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyList};

use ch_core_rs::native::decode::{decode_next_block, DecodeError, DecodeOptions};
use ch_core_rs::native::stream_decoder::StreamDecoder as RustStreamDecoder;
use ch_core_rs::native::varint::ByteReader;

#[cfg(unix)]
use std::collections::VecDeque;

#[cfg(unix)]
use ch_core_rs::batch::ColBatch as RustColBatch;

use crate::batch::ColBatch;

/// Map a core decode error to the Python exception for its failure class.
/// Unsupported input becomes NotImplementedError, matching the insert side.
/// Truncation (UnexpectedEof) becomes EOFError so callers can tell incomplete
/// data apart from corruption.
pub(crate) fn decode_err(e: DecodeError) -> PyErr {
    match e {
        DecodeError::UnsupportedType { column, type_name } => PyNotImplementedError::new_err(
            format!("Unsupported ClickHouse type '{type_name}' for column '{column}'"),
        ),
        DecodeError::InvalidBlockInfo { field_num } => {
            PyValueError::new_err(format!("Unknown BlockInfo field number {field_num}"))
        }
        DecodeError::UnsupportedSerialization {
            column,
            serialization_byte,
        } => PyNotImplementedError::new_err(format!(
            "Unsupported custom serialization (marker {serialization_byte}) for column '{column}'"
        )),
        DecodeError::BlockSchemaMismatch { block_index } => PyValueError::new_err(format!(
            "Block {block_index} schema differs from the first block"
        )),
        DecodeError::InvalidLowCardinality { column, reason } => PyValueError::new_err(format!(
            "Invalid LowCardinality layout for column '{column}': {reason}"
        )),
        DecodeError::InvalidArray { column, reason } => PyValueError::new_err(format!(
            "Invalid Array layout for column '{column}': {reason}"
        )),
        DecodeError::InvalidTuple { column, reason } => PyValueError::new_err(format!(
            "Invalid Tuple layout for column '{column}': {reason}"
        )),
        DecodeError::InvalidVariant { column, reason } => PyValueError::new_err(format!(
            "Invalid Variant layout for column '{column}': {reason}"
        )),
        DecodeError::InvalidDynamic { column, reason } => PyValueError::new_err(format!(
            "Invalid Dynamic layout for column '{column}': {reason}"
        )),
        DecodeError::Io(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
            PyEOFError::new_err(format!("Truncated Native data: {e}"))
        }
        DecodeError::Io(e) => PyRuntimeError::new_err(format!("Decode error: {e}")),
    }
}

pub(crate) fn decode_options(has_block_info: bool) -> DecodeOptions {
    DecodeOptions {
        protocol_revision: if has_block_info { 1 } else { 0 },
    }
}

/// Copy the bytes out of any u8 buffer object (bytes, bytearray, memoryview).
pub(crate) fn buffer_to_vec(data: &Bound<'_, PyAny>) -> PyResult<Vec<u8>> {
    if let Ok(bytes) = data.downcast::<PyBytes>() {
        Ok(bytes.as_bytes().to_vec())
    } else {
        PyBuffer::<u8>::get(data)?.to_vec(data.py())
    }
}

/// Incremental block decoder over a byte buffer.
///
/// Yields ColBatch objects one block at a time from an in-memory buffer.
/// Used for the materialized streaming path (buffer first, iterate blocks).
/// Every block must carry the first block's schema, the same invariant the
/// core's `decode_all_bytes` and `StreamDecoder` enforce; `decode_next_block`
/// itself is the stateless single-block primitive, so this loop applies the
/// check.
#[pyclass]
pub struct BlockDecoder {
    data: Vec<u8>,
    pos: u64,
    options: DecodeOptions,
    exhausted: bool,
    schema: Option<ch_core_rs::schema::Schema>,
    blocks_seen: usize,
}

#[pymethods]
impl BlockDecoder {
    #[new]
    #[pyo3(signature = (data, has_block_info = false))]
    fn new(data: Vec<u8>, has_block_info: bool) -> Self {
        Self {
            data,
            pos: 0,
            options: decode_options(has_block_info),
            exhausted: false,
            schema: None,
            blocks_seen: 0,
        }
    }

    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(&mut self) -> PyResult<ColBatch> {
        if self.exhausted {
            return Err(PyStopIteration::new_err(()));
        }

        let mut reader = ByteReader::new(&self.data[self.pos as usize..]);

        match decode_next_block(&mut reader, &self.options) {
            Ok(Some(batch)) => {
                match &self.schema {
                    None => self.schema = Some(batch.schema.clone()),
                    Some(first) => {
                        if batch.schema != *first {
                            self.exhausted = true;
                            return Err(decode_err(DecodeError::BlockSchemaMismatch {
                                block_index: self.blocks_seen,
                            }));
                        }
                    }
                }
                self.blocks_seen += 1;
                self.pos += reader.position() as u64;
                Ok(ColBatch::from_block(batch))
            }
            Ok(None) => {
                self.exhausted = true;
                Err(PyStopIteration::new_err(()))
            }
            Err(e) => {
                self.exhausted = true;
                Err(decode_err(e))
            }
        }
    }
}

/// Streaming block decoder that reads from a file descriptor.
///
/// Designed for true streaming: an async producer writes HTTP response chunks to
/// the write end of a pipe, and this decoder reads from the read end. Each
/// `__next__` reads a chunk from the pipe and feeds it to the push-based
/// [`RustStreamDecoder`], buffering any completed blocks and yielding them one at
/// a time. The pipe read and the decode both run with the GIL released, so the
/// async producer can keep writing concurrently.
///
/// The core decodes over in-memory slices, so the incremental read-and-feed loop
/// lives here rather than in the core: the stream decoder owns the partial-block
/// buffering and only allocates a block's columns once its last byte arrives.
///
/// Ownership: the constructor duplicates `read_fd` and reads from its own
/// duplicate, which it closes when dropped. The caller keeps ownership of
/// `read_fd` and is responsible for closing it. An invalid `read_fd` raises
/// `OSError` at construction.
///
/// Usage::
///
///     import os
///     read_fd, write_fd = os.pipe()
///     # async producer writes chunks to write_fd
///     decoder = PipeDecoder(read_fd)
///     for batch in decoder:
///         process(batch)
///     os.close(read_fd)
#[cfg(unix)]
#[pyclass]
pub struct PipeDecoder {
    reader: std::fs::File,
    decoder: RustStreamDecoder,
    /// Blocks already decoded from fed chunks but not yet yielded.
    pending: VecDeque<RustColBatch>,
    /// Reused read buffer, refilled from the pipe on each network read.
    buf: Vec<u8>,
    finished: bool,
}

#[cfg(unix)]
#[pymethods]
impl PipeDecoder {
    #[new]
    #[pyo3(signature = (read_fd, has_block_info = false))]
    fn new(read_fd: i32, has_block_info: bool) -> PyResult<Self> {
        use std::os::fd::BorrowedFd;
        if read_fd < 0 {
            return Err(PyOSError::new_err(format!(
                "invalid file descriptor: {read_fd}"
            )));
        }
        // Safety: borrow_raw requires fd != -1, guarded above. Its stays-open
        // contract is knowingly relaxed: the borrow is used only for the dup
        // call, never for I/O, so a closed fd yields EBADF from dup rather
        // than unsafety, and a recycled fd dups the caller's wrong descriptor,
        // a caller contract violation, not memory unsafety.
        let borrowed = unsafe { BorrowedFd::borrow_raw(read_fd) };
        let owned = borrowed.try_clone_to_owned()?;
        Ok(Self {
            reader: std::fs::File::from(owned),
            decoder: RustStreamDecoder::new(decode_options(has_block_info)),
            pending: VecDeque::new(),
            buf: vec![0u8; 1 << 16],
            finished: false,
        })
    }

    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(&mut self, py: Python<'_>) -> PyResult<ColBatch> {
        loop {
            if let Some(block) = self.pending.pop_front() {
                return Ok(ColBatch::from_block(block));
            }
            if self.finished {
                return Err(PyStopIteration::new_err(()));
            }

            // Read one chunk from the pipe and feed it to the stream decoder,
            // releasing the GIL across the blocking read and the decode so the
            // async producer can keep writing to the pipe concurrently. A
            // zero-length read is EOF: finish the decoder to flush trailing
            // blocks and surface a truncated final block as an error.
            let reader = &mut self.reader;
            let decoder = &mut self.decoder;
            let buf = &mut self.buf;
            let result = py.allow_threads(|| -> Result<(bool, Vec<RustColBatch>), DecodeError> {
                use std::io::Read;
                let n = reader.read(&mut buf[..])?;
                if n == 0 {
                    Ok((true, decoder.finish()?))
                } else {
                    Ok((false, decoder.feed(&buf[..n])?))
                }
            });

            match result {
                Ok((eof, blocks)) => {
                    self.finished = eof;
                    self.pending.extend(blocks);
                }
                Err(e) => {
                    self.finished = true;
                    return Err(decode_err(e));
                }
            }
        }
    }
}

/// Push-based incremental block decoder.
///
/// Receives byte chunks via `feed()` and returns any complete blocks.
/// No blocking I/O, no pipes, no threads — designed for async contexts
/// where an event loop pushes chunks as they arrive from the network.
///
/// Usage::
///
///     decoder = StreamDecoder()
///     for chunk in byte_chunks:
///         blocks = decoder.feed(chunk)
///         for block in blocks:
///             process(block)
///     final_blocks = decoder.finish()
#[pyclass]
pub struct StreamDecoder {
    inner: RustStreamDecoder,
}

#[pymethods]
impl StreamDecoder {
    #[new]
    #[pyo3(signature = (has_block_info = false))]
    fn new(has_block_info: bool) -> Self {
        let options = decode_options(has_block_info);
        Self {
            inner: RustStreamDecoder::new(options),
        }
    }

    /// Feed a chunk of bytes (bytes, bytearray, or memoryview). Returns a
    /// list of any complete ColBatch objects that could be decoded from the
    /// accumulated data.
    ///
    /// The GIL is released during the actual decode so a producer on another
    /// thread (or the asyncio event loop) can keep pulling network bytes while
    /// this call decodes — this is what makes pull and parse overlap.
    fn feed<'py>(
        &mut self,
        py: Python<'py>,
        data: &Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyList>> {
        // Copy out of Python-owned memory so we can drop the GIL during decode.
        let owned = buffer_to_vec(data)?;
        let batches = py
            .allow_threads(|| self.inner.feed(&owned))
            .map_err(decode_err)?;
        let py_batches: Vec<ColBatch> = batches.into_iter().map(ColBatch::from_block).collect();
        PyList::new(py, py_batches)
    }

    /// Signal end of stream. Returns any remaining complete blocks.
    /// Raises an error if the stream ends with a truncated block.
    fn finish<'py>(&mut self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        let batches = py
            .allow_threads(|| self.inner.finish())
            .map_err(decode_err)?;
        let py_batches: Vec<ColBatch> = batches.into_iter().map(ColBatch::from_block).collect();
        PyList::new(py, py_batches)
    }
}
