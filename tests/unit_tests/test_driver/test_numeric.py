import unittest
from unittest.mock import Mock

import numpy as np

from clickhouse_connect.datatypes.base import TypeDef
from clickhouse_connect.datatypes.numeric import BFloat16
from clickhouse_connect.driver.buffer import ResponseBuffer


def create_test_source(data: bytes):
    """Create a ResponseBuffer from test data using a simple generator source."""

    class SimpleSource:
        def __init__(self, payload: bytes):
            self.gen = iter([payload])

        def close(self):
            pass

    return ResponseBuffer(SimpleSource(data))


class _QueryCtx(Mock):
    """Cheap stand-in for QueryContext with only the attrs we touch."""

    def __init__(
        self,
        *,
        use_numpy=False,
        use_none=True,
        use_extended_dtypes=False,
        as_pandas=False,
    ):
        super().__init__()
        self.use_numpy = use_numpy
        self.use_none = use_none
        self.use_extended_dtypes = use_extended_dtypes
        self.as_pandas = as_pandas


class _InsertCtx(Mock):
    """Cheap stand-in for InsertContext (only needs a column_name)."""

    def __init__(self):
        super().__init__()
        self.column_name = "bf16_col"


# pylint: disable=protected-access
class TestBFloat16(unittest.TestCase):

    def setUp(self):
        self.bf16 = BFloat16(TypeDef())
        self.null_bf16 = BFloat16(TypeDef(wrappers=("Nullable",)))
        self.ins_ctx = _InsertCtx()
        self.qry_ctx = _QueryCtx()

    def test_roundtrip_non_nullable(self):
        """Test write -> read non-nullable."""
        data_in = [5.5, 17.888884]
        expected = [5.5, 17.875]
        dest = bytearray()
        self.bf16._write_column_binary(data_in, dest, self.ins_ctx)
        source = create_test_source(bytes(dest))
        out = self.bf16._read_column_binary(source, 2, self.qry_ctx, None)
        self.assertEqual(out, expected)

    def test_roundtrip_nullable(self):
        """Test write -> read nullable path"""
        data_in = [5.5, None, 17.888884]
        expected = [5.5, None, 17.875]
        dest = bytearray()
        # Manually handle null map
        dest.extend([1 if v is None else 0 for v in data_in])
        self.null_bf16._write_column_binary(data_in, dest, self.ins_ctx)
        source = create_test_source(bytes(dest))
        out = self.null_bf16._read_nullable_column(
            source, len(data_in), self.qry_ctx, None
        )
        self.assertEqual(out, expected)

    def test_numpy_fastpath(self):
        """Test vectorized numpy path"""
        self.qry_ctx.use_numpy = True
        data = [3.141592, -2.71828]
        expected = [3.140625, -2.703125]
        bf16 = BFloat16(TypeDef())
        data_f32 = np.array(data, dtype=np.float32)
        dest = bytearray()
        bf16._write_column_binary(data_f32.tolist(), dest, self.ins_ctx)
        source = create_test_source(bytes(dest))
        out = bf16._read_column_binary(source, len(data_f32), self.qry_ctx, None)
        self.assertIsInstance(out, np.ndarray)
        self.assertEqual(out.dtype, np.float32)

        expected = np.array(expected, dtype=np.float32)
        self.assertTrue(np.all(out == expected))

    def test_encoded_size(self):
        "Test we only encode 2 bytes per float"
        sample = [10.0, 20.0, 30.0]
        dest = bytearray()
        self.bf16._write_column_binary(sample, dest, self.ins_ctx)
        self.assertEqual(len(dest), 2 * len(sample))


if __name__ == "__main__":
    unittest.main()
