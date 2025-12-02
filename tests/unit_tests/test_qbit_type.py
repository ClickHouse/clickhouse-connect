import math

import pytest

import clickhouse_connect.datatypes.vector as vector_module
from clickhouse_connect.datatypes.registry import get_from_name
from clickhouse_connect.datatypes.vector import QBit
from clickhouse_connect.driver.options import np

# pylint: disable=protected-access


def test_qbit_type_registration():
    """Test that QBit type is properly registered"""
    qbit = get_from_name("QBit(Float32, 8)")
    assert isinstance(qbit, QBit)
    assert qbit.element_type == "Float32"
    assert qbit.dimension == 8


def test_qbit_float32_tuple_structure():
    """Test that QBit(Float32, 8) creates correct underlying tuple"""
    qbit = get_from_name("QBit(Float32, 8)")
    assert len(qbit._tuple_type.element_types) == 32
    assert all(e.name == "FixedString(1)" for e in qbit._tuple_type.element_types)


def test_qbit_bfloat16_tuple_structure():
    """Test that QBit(BFloat16, 16) creates correct underlying tuple"""
    qbit = get_from_name("QBit(BFloat16, 16)")
    assert len(qbit._tuple_type.element_types) == 16
    assert all(e.name == "FixedString(2)" for e in qbit._tuple_type.element_types)


def test_qbit_float64_tuple_structure():
    """Test that QBit(Float64, 64) creates correct underlying tuple"""
    qbit = get_from_name("QBit(Float64, 64)")
    assert len(qbit._tuple_type.element_types) == 64
    assert all(e.name == "FixedString(8)" for e in qbit._tuple_type.element_types)


def test_qbit_type_name():
    """Test that QBit type name is correctly formatted"""
    qbit1 = get_from_name("QBit(Float32, 8)")
    assert qbit1.name == "QBit(Float32, 8)"

    qbit2 = get_from_name("QBit(BFloat16, 128)")
    assert qbit2.name == "QBit(BFloat16, 128)"

    qbit3 = get_from_name("QBit(Float64, 256)")
    assert qbit3.name == "QBit(Float64, 256)"


def test_qbit_type_caching():
    """Test that QBit types are cached properly"""
    qbit1 = get_from_name("QBit(Float32, 8)")
    qbit2 = get_from_name("QBit(Float32, 8)")
    assert qbit1 is qbit2


def test_qbit_different_dimensions():
    """Test QBit with various dimension values"""
    test_cases = [
        (4, 1),
        (8, 1),
        (16, 2),
        (32, 4),
        (64, 8),
        (128, 16),
    ]
    for elm_bits in [16, 32, 64]:
        for dim, expected_bytes in test_cases:
            b = "B" if elm_bits == 16 else ""
            qbit = get_from_name(f"QBit({b}Float{elm_bits}, {dim})")
            assert qbit.dimension == dim
            # All QBit(Float<elm_bits>, *) should have <elm_bits> FixedString columns
            assert len(qbit._tuple_type.element_types) == elm_bits
            # FixedString size should be ceil(dimension / 8)
            assert all(e.name == f"FixedString({expected_bytes})" for e in qbit._tuple_type.element_types)


def test_qbit_delegation_methods():
    """Test that QBit has delegation methods for read/write operations"""
    element_types = ["BFloat16", "Float32", "Float64"]
    for el_type in element_types:
        qbit = get_from_name(f"QBit({el_type}, 8)")
        assert hasattr(qbit, "read_column_prefix")
        assert hasattr(qbit, "read_column_data")
        assert hasattr(qbit, "write_column_prefix")
        assert hasattr(qbit, "write_column_data")
        assert callable(qbit.read_column_prefix)
        assert callable(qbit.read_column_data)
        assert callable(qbit.write_column_prefix)
        assert callable(qbit.write_column_data)


def test_invalid_element_types():
    """Test that invalid element types fail."""
    test_cases = ["Int32", "String", "DateTime"]

    for case in test_cases:
        with pytest.raises(ValueError):
            get_from_name(f"QBit({case}, 8)")


def test_invalid_dimensions():
    """Test that invalid dimensions fail."""
    test_cases = [0, -5]

    for case in test_cases:
        with pytest.raises(ValueError):
            get_from_name(f"QBit(Float32, {case})")


def test_transpose_untranspose_roundtrip_float32():
    """Test that transpose -> untranspose is identity for Float32"""
    qbit = get_from_name("QBit(Float32, 8)")

    test_vectors = [
        [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0],
        [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
        [-1.5, -2.5, -3.5, -4.5, -5.5, -6.5, -7.5, -8.5],
        [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8],
        [1000.0, 2000.0, 3000.0, 4000.0, 5000.0, 6000.0, 7000.0, 8000.0],
        [1.5, -2.5, 3.5, -4.5, 5.5, -6.5, 7.5, -8.5],
    ]

    for original in test_vectors:
        transposed = qbit._transpose_row(original)
        assert isinstance(transposed, tuple)
        assert len(transposed) == 32

        for bit_plane in transposed:
            assert isinstance(bit_plane, bytes)
            assert len(bit_plane) == 1

        result = qbit._untranspose_row(transposed)
        assert len(result) == len(original)
        assert result == pytest.approx(original, rel=1e-6)


def test_transpose_untranspose_roundtrip_float64():
    """Test that transpose -> untranspose is identity for Float64"""
    qbit = get_from_name("QBit(Float64, 5)")

    test_vectors = [
        [-0.99105519, 1.28887844, -0.43526649, -0.98520696, 0.66154391],  # apple
        [-0.69372815, 0.25587061, -0.88226235, -2.54593015, 0.05300475],  # banana
        [0.93338752, 2.06571317, -0.54612565, -1.51625717, 0.69775337],  # orange
    ]

    for original in test_vectors:
        transposed = qbit._transpose_row(original)
        assert isinstance(transposed, tuple)
        assert len(transposed) == 64

        for bit_plane in transposed:
            assert isinstance(bit_plane, bytes)
            assert len(bit_plane) == 1

        result = qbit._untranspose_row(transposed)
        assert len(result) == len(original)
        assert result == pytest.approx(original, abs=1e-15)


def test_transpose_untranspose_roundtrip_bfloat16():
    """Test that transpose -> untranspose works for BFloat16"""
    qbit = get_from_name("QBit(BFloat16, 8)")

    test_vectors = [
        [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0],
        [0.5, 1.5, 2.5, 3.5, 4.5, 5.5, 6.5, 7.5],
        [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
    ]

    for original in test_vectors:
        transposed = qbit._transpose_row(original)
        assert isinstance(transposed, tuple)
        assert len(transposed) == 16

        for bit_plane in transposed:
            assert isinstance(bit_plane, bytes)
            assert len(bit_plane) == 1

        result = qbit._untranspose_row(transposed)
        assert len(result) == len(original)
        assert result == pytest.approx(original, rel=1e-2, abs=1.5e-2)


def test_transpose_specific_bit_pattern():
    """Test transposition with known bit patterns to verify correctness"""
    qbit = get_from_name("QBit(Float32, 4)")

    vector = [1.0, 1.0, 1.0, 1.0]
    transposed = qbit._transpose_row(vector)
    assert len(transposed) == 32

    for bit_plane in transposed:
        assert len(bit_plane) == 1

    expected_bits = []
    float_bits = 0x3F800000
    for bit_idx in range(32):
        bit_pos = 31 - bit_idx  # MSB first
        if float_bits & (1 << bit_pos):
            expected_bits.append(0x0F)  # All 4 bits set
        else:
            expected_bits.append(0x00)  # All 4 bits clear

    for idx, bit_plane in enumerate(transposed):
        assert bit_plane[0] == expected_bits[idx]


def test_transpose_zeros():
    """Test that zero vectors transpose correctly"""
    qbit = get_from_name("QBit(Float32, 8)")

    zeros = [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]
    transposed = qbit._transpose_row(zeros)

    for bit_plane in transposed:
        assert bit_plane == b"\x00"

    result = qbit._untranspose_row(transposed)
    assert result == pytest.approx(zeros, abs=1e-10)


def test_transpose_large_dimension():
    """Test transposition with larger dimension to ensure byte alignment works"""
    qbit = get_from_name("QBit(Float32, 128)")

    vector = [float(i) for i in range(128)]
    transposed = qbit._transpose_row(vector)
    assert len(transposed) == 32

    for bit_plane in transposed:
        assert len(bit_plane) == 16

    result = qbit._untranspose_row(transposed)
    assert result == pytest.approx(vector, rel=1e-6)


def test_untranspose_manual_bit_pattern():
    """Test untranspose with a manually constructed bit pattern"""
    qbit = get_from_name("QBit(Float32, 2)")

    # Manually construct bit planes for [1.0, 2.0]
    # 1.0 in Float32 is 0x3F800000 = 0b00111111100000000000000000000000
    # 2.0 in Float32 is 0x40000000 = 0b01000000000000000000000000000000

    # Create 32 bit planes, each with ceil(2/8) = 1 byte
    # For 2 elements, we pack them in bits 0 and 1 of each byte
    bit_planes = []

    val1_bits = 0x3F800000
    val2_bits = 0x40000000

    for bit_idx in range(32):
        bit_pos = 31 - bit_idx  # MSB first

        # Extract bit from each value
        bit1 = 1 if (val1_bits & (1 << bit_pos)) else 0
        bit2 = 1 if (val2_bits & (1 << bit_pos)) else 0

        # Pack into byte: bit 0 = element 0, bit 1 = element 1
        byte_val = (bit2 << 1) | bit1
        bit_planes.append(bytes([byte_val]))

    transposed = tuple(bit_planes)

    result = qbit._untranspose_row(transposed)
    assert len(result) == 2
    assert result[0] == pytest.approx(1.0, rel=1e-6)
    assert result[1] == pytest.approx(2.0, rel=1e-6)


def test_transpose_different_element_types():
    """Test transposition works correctly for all supported element types"""
    test_cases = [
        # (Elem type, dimension, planes, tol)
        ("Float32", 8, 32, 1e-6),
        ("Float64", 8, 64, 1e-15),
        ("BFloat16", 8, 16, 1e-2),
    ]

    for elem_type, dim, expected_planes, tolerance in test_cases:
        qbit = get_from_name(f"QBit({elem_type}, {dim})")

        vector = [float(i + 1) for i in range(dim)]
        transposed = qbit._transpose_row(vector)
        assert len(transposed) == expected_planes

        result = qbit._untranspose_row(transposed)
        if elem_type == "Float64":
            assert result == pytest.approx(vector, abs=tolerance)
        else:
            assert result == pytest.approx(vector, rel=tolerance)


def test_transpose_dimension_mismatch_too_short():
    """Test that transposing a vector that's too short raises ValueError"""
    qbit = get_from_name("QBit(Float32, 8)")

    short_vector = [1.0, 2.0, 3.0, 4.0, 5.0]

    with pytest.raises(ValueError) as exc_info:
        qbit._transpose_row(short_vector)

    assert "dimension mismatch" in str(exc_info.value).lower()
    assert "expected 8" in str(exc_info.value)
    assert "got 5" in str(exc_info.value)


def test_transpose_dimension_mismatch_too_long():
    """Test that transposing a vector that's too long raises ValueError"""
    qbit = get_from_name("QBit(Float32, 8)")

    long_vector = [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0]

    with pytest.raises(ValueError) as exc_info:
        qbit._transpose_row(long_vector)

    assert "dimension mismatch" in str(exc_info.value).lower()
    assert "expected 8" in str(exc_info.value)
    assert "got 12" in str(exc_info.value)


def test_transpose_dimension_mismatch_empty():
    """Test that transposing an empty vector raises ValueError"""
    qbit = get_from_name("QBit(Float32, 8)")

    with pytest.raises(ValueError) as exc_info:
        qbit._transpose_row([])

    assert "dimension mismatch" in str(exc_info.value).lower()
    assert "expected 8" in str(exc_info.value)
    assert "got 0" in str(exc_info.value)


def test_transpose_special_float_values_inf():
    """Test that infinity values are handled correctly"""
    qbit = get_from_name("QBit(Float32, 4)")

    inf_vector = [float("inf"), float("-inf"), 1.0, 2.0]
    transposed = qbit._transpose_row(inf_vector)

    result = qbit._untranspose_row(transposed)
    assert result[0] == float("inf")
    assert result[1] == float("-inf")
    assert result[2] == pytest.approx(1.0)
    assert result[3] == pytest.approx(2.0)


def test_transpose_special_float_values_nan():
    """Test that NaN values are handled (round-trip produces NaN)"""
    qbit = get_from_name("QBit(Float32, 4)")

    nan_vector = [float("nan"), 1.0, 2.0, 3.0]
    transposed = qbit._transpose_row(nan_vector)

    result = qbit._untranspose_row(transposed)

    assert math.isnan(result[0])
    assert result[1] == pytest.approx(1.0)
    assert result[2] == pytest.approx(2.0)
    assert result[3] == pytest.approx(3.0)


def test_transpose_special_float_values_all_inf():
    """Test vector with all infinity values"""
    qbit = get_from_name("QBit(Float32, 4)")

    inf_vector = [float("inf")] * 4
    transposed = qbit._transpose_row(inf_vector)

    result = qbit._untranspose_row(transposed)
    assert all(x == float("inf") for x in result)


def test_transpose_special_float_values_mixed_special():
    """Test vector with mixed special values (inf, -inf, nan, zero, normal)"""
    qbit = get_from_name("QBit(Float64, 8)")

    mixed_vector = [float("inf"), float("-inf"), float("nan"), 0.0, -0.0, 1.5, -2.5, 1e100]
    transposed = qbit._transpose_row(mixed_vector)

    result = qbit._untranspose_row(transposed)
    assert result[0] == float("inf")
    assert result[1] == float("-inf")
    assert math.isnan(result[2])
    assert result[3] == 0.0
    assert result[4] == -0.0
    assert result[5] == pytest.approx(1.5)
    assert result[6] == pytest.approx(-2.5)
    assert result[7] == pytest.approx(1e100)


def test_qbit_dimension_one():
    """Test QBit with minimum dimension (1)"""
    qbit = get_from_name("QBit(Float32, 1)")

    assert qbit.dimension == 1
    assert len(qbit._tuple_type.element_types) == 32

    vector = [40.0]
    transposed = qbit._transpose_row(vector)
    result = qbit._untranspose_row(transposed)
    assert result == pytest.approx(vector, rel=1e-6)


def test_qbit_dimension_not_multiple_of_8():
    """Test QBit with dimension that's not a multiple of 8"""
    test_cases = [3, 5, 7, 9, 13, 17]

    for dim in test_cases:
        qbit = get_from_name(f"QBit(Float32, {dim})")
        assert qbit.dimension == dim

        vector = [float(i + 1) for i in range(dim)]
        transposed = qbit._transpose_row(vector)
        result = qbit._untranspose_row(transposed)
        assert result == pytest.approx(vector, rel=1e-6)


def test_qbit_very_large_dimension():
    """Test QBit with very large dimension (512)"""
    qbit = get_from_name("QBit(Float32, 512)")

    assert qbit.dimension == 512
    # Each bit plane should be ceil(512/8) = 64 bytes
    assert all(e.name == "FixedString(64)" for e in qbit._tuple_type.element_types)

    vector = [float(i % 100) for i in range(512)]
    transposed = qbit._transpose_row(vector)
    result = qbit._untranspose_row(transposed)
    assert result == pytest.approx(vector, rel=1e-6)


def test_qbit_dimension_power_of_2():
    """Test QBit with various power-of-2 dimensions"""
    for power in [0, 1, 2, 3, 4, 5, 6, 7]:
        dim = 2**power
        qbit = get_from_name(f"QBit(Float32, {dim})")
        assert qbit.dimension == dim

        vector = [float(i) for i in range(dim)]
        transposed = qbit._transpose_row(vector)
        result = qbit._untranspose_row(transposed)
        assert result == pytest.approx(vector, rel=1e-6)


def test_transpose_very_small_floats():
    """Test transposition with very small float values (near zero)"""
    qbit = get_from_name("QBit(Float64, 4)")

    # Test very small values (subnormal floats)
    small_vector = [1e-300, -1e-300, 1e-308, -1e-308]
    transposed = qbit._transpose_row(small_vector)

    result = qbit._untranspose_row(transposed)
    assert result == pytest.approx(small_vector, abs=1e-315)


def test_transpose_very_large_floats():
    """Test transposition with very large float values"""
    qbit = get_from_name("QBit(Float64, 4)")

    # Test very large values (near max float)
    large_vector = [1e100, -1e100, 1e200, -1e200]
    transposed = qbit._transpose_row(large_vector)

    result = qbit._untranspose_row(transposed)
    assert result == pytest.approx(large_vector, rel=1e-10)


def test_transpose_negative_zero():
    """Test that negative zero is preserved"""
    qbit = get_from_name("QBit(Float32, 4)")

    vector = [0.0, -0.0, 1.0, -1.0]
    transposed = qbit._transpose_row(vector)
    result = qbit._untranspose_row(transposed)
    assert math.copysign(1, result[0]) == 1
    assert math.copysign(1, result[1]) == -1
    assert result[2] == pytest.approx(1.0)
    assert result[3] == pytest.approx(-1.0)


def test_values_to_words_known_patterns():
    """Test float to int conversion with known IEEE 754 bit patterns"""
    qbit_f32 = get_from_name("QBit(Float32, 4)")
    qbit_f64 = get_from_name("QBit(Float64, 2)")
    qbit_bf16 = get_from_name("QBit(BFloat16, 4)")

    # Float32: Test known bit patterns
    # 1.0 = 0x3F800000, 2.0 = 0x40000000, 0.0 = 0x00000000, -1.0 = 0xBF800000
    words_f32 = qbit_f32._values_to_words([1.0, 2.0, 0.0, -1.0])
    assert words_f32[0] == 0x3F800000
    assert words_f32[1] == 0x40000000
    assert words_f32[2] == 0x00000000
    assert words_f32[3] == 0xBF800000

    # Float64: Test known bit patterns
    # 1.0 = 0x3FF0000000000000, -1.0 = 0xBFF0000000000000
    words_f64 = qbit_f64._values_to_words([1.0, -1.0])
    assert words_f64[0] == 0x3FF0000000000000
    assert words_f64[1] == 0xBFF0000000000000

    # BFloat16: Top 16 bits of Float32
    # 1.0 Float32 = 0x3F800000 -> BFloat16 = 0x3F80
    # 2.0 Float32 = 0x40000000 -> BFloat16 = 0x4000
    words_bf16 = qbit_bf16._values_to_words([1.0, 2.0, 0.0, -1.0])
    assert words_bf16[0] == 0x3F80
    assert words_bf16[1] == 0x4000
    assert words_bf16[2] == 0x0000
    assert words_bf16[3] == 0xBF80


def test_words_to_values_known_patterns():
    """Test int to float conversion with known IEEE 754 bit patterns"""
    qbit_f32 = get_from_name("QBit(Float32, 4)")
    qbit_f64 = get_from_name("QBit(Float64, 2)")
    qbit_bf16 = get_from_name("QBit(BFloat16, 4)")

    # Float32: Known bit patterns -> floats
    words_f32 = [0x3F800000, 0x40000000, 0x00000000, 0xBF800000]
    values_f32 = qbit_f32._words_to_values(words_f32)
    assert values_f32[0] == pytest.approx(1.0)
    assert values_f32[1] == pytest.approx(2.0)
    assert values_f32[2] == pytest.approx(0.0)
    assert values_f32[3] == pytest.approx(-1.0)

    # Float64: Known bit patterns -> floats
    words_f64 = [0x3FF0000000000000, 0xBFF0000000000000]
    values_f64 = qbit_f64._words_to_values(words_f64)
    assert values_f64[0] == pytest.approx(1.0)
    assert values_f64[1] == pytest.approx(-1.0)

    # BFloat16: 16-bit words -> floats (expanded to Float32)
    words_bf16 = [0x3F80, 0x4000, 0x0000, 0xBF80]
    values_bf16 = qbit_bf16._words_to_values(words_bf16)
    assert values_bf16[0] == pytest.approx(1.0, rel=1e-2)
    assert values_bf16[1] == pytest.approx(2.0, rel=1e-2)
    assert values_bf16[2] == pytest.approx(0.0, abs=1e-2)
    assert values_bf16[3] == pytest.approx(-1.0, rel=1e-2)


@pytest.mark.skipif(np is None, reason="Numpy not available")
def test_numpy_array_input_output():
    """Test that numpy arrays use fast path and return correct Python types"""
    qbit = get_from_name("QBit(Float32, 8)")

    np_vector = np.array([1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0], dtype=np.float32)
    transposed = qbit._transpose_row(np_vector)
    assert isinstance(transposed, tuple)
    assert len(transposed) == 32

    for bit_plane in transposed:
        assert isinstance(bit_plane, bytes)
    result = qbit._untranspose_row(transposed)

    assert isinstance(result, list)
    assert result == pytest.approx([1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0])

    test_cases = [
        ("Float32", np.float32, 1e-6),
        ("Float64", np.float64, 1e-15),
        ("BFloat16", np.float32, 1e-2),  # BFloat16 input is Float32
    ]

    for elem_type, dtype, tolerance in test_cases:
        qb = get_from_name(f"QBit({elem_type}, 4)")
        vec = np.array([1.0, 2.0, 3.0, 4.0], dtype=dtype)

        trans = qb._transpose_row(vec)
        res = qb._untranspose_row(trans)

        assert isinstance(res, list)
        assert isinstance(trans, tuple)

        if elem_type == "Float64":
            assert res == pytest.approx([1.0, 2.0, 3.0, 4.0], abs=tolerance)
        else:
            assert res == pytest.approx([1.0, 2.0, 3.0, 4.0], rel=tolerance)


@pytest.mark.skipif(np is None, reason="Numpy not available")
def test_transpose_numpy_vs_pure_python_equivalence():
    """Test that pure Python and numpy transpose produce identical results"""

    test_cases = [
        ("Float32", 8, [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0]),
        ("Float32", 16, [float(i) for i in range(16)]),
        ("Float32", 128, [float(i % 10) for i in range(128)]),
        ("Float64", 8, [1.5, -2.5, 3.5, -4.5, 5.5, -6.5, 7.5, -8.5]),
        ("BFloat16", 8, [0.5, 1.5, 2.5, 3.5, 4.5, 5.5, 6.5, 7.5]),
    ]

    for elem_type, dimension, test_vector in test_cases:
        qbit = get_from_name(f"QBit({elem_type}, {dimension})")

        # Get pure Python result by temporarily disabling numpy
        original_np = vector_module.np
        try:
            # Disable numpy to force pure Python path
            vector_module.np = None
            result_python = qbit._transpose_row(test_vector)
        finally:
            vector_module.np = original_np

        dtype = np.float64 if elem_type == "Float64" else np.float32
        np_vector = np.array(test_vector, dtype=dtype)
        result_numpy = qbit._transpose_row_numpy(np_vector)

        assert len(result_python) == len(result_numpy)

        for plane_py, plane_np in zip(result_python, result_numpy):
            assert plane_py == plane_np


@pytest.mark.skipif(np is None, reason="Numpy not available")
def test_untranspose_numpy_vs_pure_python_equivalence():
    """Test that pure Python and numpy untranspose produce identical results"""

    test_cases = [
        ("Float32", 8, [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0]),
        ("Float32", 16, [float(i) for i in range(16)]),
        ("Float32", 128, [float(i % 10) for i in range(128)]),
        ("Float64", 8, [1.5, -2.5, 3.5, -4.5, 5.5, -6.5, 7.5, -8.5]),
        ("BFloat16", 8, [0.5, 1.5, 2.5, 3.5, 4.5, 5.5, 6.5, 7.5]),
    ]

    for elem_type, dimension, test_vector in test_cases:
        qbit = get_from_name(f"QBit({elem_type}, {dimension})")

        dtype = np.float64 if elem_type == "Float64" else np.float32
        np_vector = np.array(test_vector, dtype=dtype)
        bit_planes = qbit._transpose_row_numpy(np_vector)

        # Get pure Python result by temporarily disabling numpy
        original_np = vector_module.np
        try:
            # Disable numpy to force pure Python path
            vector_module.np = None
            result_python = qbit._untranspose_row(bit_planes)
        finally:
            vector_module.np = original_np

        result_numpy = qbit._untranspose_row_numpy(bit_planes)
        assert len(result_python) == len(result_numpy)
