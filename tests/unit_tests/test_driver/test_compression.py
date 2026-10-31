import importlib
import sys

import pytest

from clickhouse_connect.driver import compression
from clickhouse_connect.driver.compression import _zstd_compress, _zstd_decompress, _zstd_decompressor


def test_zstd_round_trip():
    data = b"clickhouse zstd round trip " * 100
    assert _zstd_decompress(_zstd_compress(data)) == data


def test_zstd_incremental_decompression():
    data = b"clickhouse zstd incremental " * 100
    compressed = _zstd_compress(data)
    decompressor = _zstd_decompressor()
    out = b"".join(decompressor.decompress(compressed[i : i + 64]) for i in range(0, len(compressed), 64))
    assert out == data


def test_zstd_unavailable(monkeypatch):
    monkeypatch.setattr(compression, "_zstd", None)
    with pytest.raises(ImportError, match="zstd support is unavailable"):
        compression._zstd_compress(b"data")
    with pytest.raises(ImportError, match="zstd support is unavailable"):
        compression._zstd_decompress(b"data")
    with pytest.raises(ImportError, match="zstd support is unavailable"):
        compression._zstd_decompressor()


def test_lz4_unavailable(monkeypatch):
    monkeypatch.setattr(compression, "_lz4_frame", None)
    with pytest.raises(ImportError, match="requires the 'lz4' package"):
        compression._lz4_decompressor()
    with pytest.raises(ImportError, match="requires the 'lz4' package"):
        compression.Lz4Compressor()


def test_lz4_missing_at_import(monkeypatch):
    monkeypatch.setitem(sys.modules, "lz4", None)
    monkeypatch.setitem(sys.modules, "lz4.frame", None)
    reloaded = importlib.reload(compression)
    try:
        assert "lz4" not in reloaded.available_compression
        assert reloaded.available_compression[0] != "lz4"
    finally:
        monkeypatch.undo()
        importlib.reload(compression)
