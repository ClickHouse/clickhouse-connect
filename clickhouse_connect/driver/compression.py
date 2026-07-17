import sys
import zlib

import lz4
import lz4.frame

if sys.version_info >= (3, 14):
    from compression import zstd as _zstd
else:
    from backports import zstd as _zstd

try:
    import brotli
except ImportError:
    brotli = None


ZstdError = _zstd.ZstdError


def zstd_compress(data: bytes) -> bytes:
    """One-shot compression."""
    return _zstd.compress(data)


def zstd_decompress(data: bytes) -> bytes:
    """One-shot decompression."""
    return _zstd.decompress(data)


def zstd_decompressor():
    """Returns a ZstdDecompressor for incremental decompression."""
    return _zstd.ZstdDecompressor()


available_compression = ["lz4", "zstd"]

if brotli:
    available_compression.append("br")
available_compression.extend(["gzip", "deflate"])

comp_map: dict[str, "Compressor | type[Compressor]"] = {}


class Compressor:
    def __init_subclass__(cls, tag: str, thread_safe: bool = True):
        comp_map[tag] = cls() if thread_safe else cls

    def compress_block(self, block) -> bytes | bytearray:
        return block

    def flush(self):
        pass


class GzipCompressor(Compressor, tag="gzip", thread_safe=False):
    def __init__(self, level: int = 6, wbits: int = 31):
        self.zlib_obj = zlib.compressobj(level=level, wbits=wbits)

    def compress_block(self, block):
        return self.zlib_obj.compress(block)

    def flush(self):
        return self.zlib_obj.flush()


class Lz4Compressor(Compressor, tag="lz4", thread_safe=False):
    def __init__(self):
        self.comp = lz4.frame.LZ4FrameCompressor()

    def compress_block(self, block):
        output = self.comp.begin(len(block))
        output += self.comp.compress(block)
        return output + self.comp.flush()


class ZstdCompressor(Compressor, tag="zstd"):
    def compress_block(self, block):
        return zstd_compress(block)


class BrotliCompressor(Compressor, tag="br"):
    def compress_block(self, block):
        return brotli.compress(block)


null_compressor = Compressor()


def get_compressor(compression: str | None) -> Compressor:
    if not compression:
        return null_compressor
    comp = comp_map[compression]
    if isinstance(comp, Compressor):
        return comp
    return comp()
