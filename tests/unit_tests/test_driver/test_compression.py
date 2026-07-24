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
