import importlib
import sys

import pytest

import clickhouse_connect
from clickhouse_connect.driver import compression
from clickhouse_connect.driver._backend import httpcommon
from clickhouse_connect.driver.compression import _zstd_compress, _zstd_decompress, _zstd_decompressor
from clickhouse_connect.driver.exceptions import ProgrammingError


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
    with pytest.raises(ImportError, match=r"clickhouse-connect\[lz4\]"):
        compression._lz4_decompressor()
    with pytest.raises(ImportError, match=r"clickhouse-connect\[lz4\]"):
        compression.Lz4Compressor()


def test_explicit_lz4_without_package(monkeypatch):
    monkeypatch.setattr(httpcommon, "available_compression", ["zstd", "gzip", "deflate"])
    with pytest.raises(ProgrammingError, match=r"clickhouse-connect\[lz4\]"):
        clickhouse_connect.get_client(host="localhost", compress="lz4")


def test_lz4_missing_at_import(monkeypatch):
    monkeypatch.setitem(sys.modules, "lz4", None)
    monkeypatch.setitem(sys.modules, "lz4.frame", None)
    reloaded = importlib.reload(compression)
    try:
        assert "lz4" not in reloaded.available_compression
    finally:
        monkeypatch.undo()
        importlib.reload(compression)
