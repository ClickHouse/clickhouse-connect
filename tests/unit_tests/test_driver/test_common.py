import struct

import pytest

from clickhouse_connect.driver.common import write_array
from clickhouse_connect.driver.exceptions import DataError


@pytest.mark.parametrize("code", "bBhHiIlLqQ")
@pytest.mark.parametrize("value", [None, 1.5, [], {}, "13"])
def test_write_array_integer_type_mismatch(code, value):
    dest = bytearray(b"prefix")
    with pytest.raises(DataError) as exc:
        write_array(code, [13, value], dest, "value")
    assert str(exc.value) == ("Unable to create native array for column `value`: type mismatch (usually None in non-Nullable column)")
    assert isinstance(exc.value.__cause__, struct.error)
    assert dest == b"prefix"


@pytest.mark.parametrize("code", "bBhHiIlLqQ")
@pytest.mark.parametrize("overflow", ["below", "above", "huge_negative", "huge_positive"])
def test_write_array_integer_out_of_range(code, overflow):
    bits = struct.calcsize(f"<{code}") * 8
    lower = -(1 << (bits - 1)) if code.islower() else 0
    upper = (1 << (bits - 1)) - 1 if code.islower() else (1 << bits) - 1
    value = {"below": lower - 1, "above": upper + 1, "huge_negative": -(1 << 100), "huge_positive": 1 << 100}[overflow]
    dest = bytearray(b"prefix")
    with pytest.raises(DataError) as exc:
        write_array(code, [13, value], dest)
    assert str(exc.value) == "Unable to create native array: value out of range"
    assert isinstance(exc.value.__cause__, (OverflowError, struct.error))
    assert dest == b"prefix"


@pytest.mark.parametrize("code", "bBhHiIlLqQ")
def test_write_array_integer_bounds(code):
    bits = struct.calcsize(f"<{code}") * 8
    lower = -(1 << (bits - 1)) if code.islower() else 0
    upper = (1 << (bits - 1)) - 1 if code.islower() else (1 << bits) - 1
    dest = bytearray(b"prefix")
    write_array(code, [lower, 13, upper], dest)
    assert dest == b"prefix" + struct.pack(f"<3{code}", lower, 13, upper)


def test_write_array_preserves_type_error():
    error = TypeError("index failed")

    class InvalidIndex:
        def __index__(self):
            raise error

    with pytest.raises(DataError) as exc:
        write_array("i", [InvalidIndex()], bytearray())
    assert str(exc.value) == "Unable to create native array: type mismatch (usually None in non-Nullable column)"
    assert exc.value.__cause__ is error


def test_write_array_preserves_overflow_error():
    with pytest.raises(DataError) as exc:
        write_array("f", [1e100], bytearray())
    assert str(exc.value) == "Unable to create native array: value out of range"
    assert isinstance(exc.value.__cause__, OverflowError)


@pytest.mark.parametrize(
    ("code", "column", "detail"),
    [
        ("z", [13], "bad char in struct format"),
        ("2b", [13], "pack expected 12 items for packing (got 1)"),
        ("f", [None], "required argument is not a float"),
    ],
)
def test_write_array_other_struct_errors(code, column, detail):
    with pytest.raises(DataError) as exc:
        write_array(code, column, bytearray())
    assert str(exc.value) == f"Unable to create native array: {detail}"
    assert isinstance(exc.value.__cause__, struct.error)


@pytest.fixture(params=["clickhouse_connect.driver.dataconv", "clickhouse_connect.driverc.dataconv"], ids=["python", "cython"])
def native_writer(request):
    return pytest.importorskip(request.param).write_native_col


@pytest.mark.parametrize("code", "bBhHiIlLqQ")
@pytest.mark.parametrize(
    ("value", "detail"),
    [
        (None, "type mismatch (usually None in non-Nullable column)"),
        (-(1 << 100), "value out of range"),
        (1 << 100, "value out of range"),
    ],
)
@pytest.mark.parametrize("col_name", [None, "value"])
def test_write_native_col_integer_errors(native_writer, code, value, detail, col_name):
    dest = bytearray(b"prefix")
    with pytest.raises(DataError) as exc:
        native_writer(code, [13, value], dest, col_name)
    col_msg = f" for column `{col_name}`" if col_name else ""
    assert str(exc.value) == f"Unable to create native array{col_msg}: {detail}"
    assert isinstance(exc.value.__cause__, (struct.error, OverflowError))
    assert dest == b"prefix"


@pytest.mark.parametrize(
    ("code", "value", "detail", "cause"),
    [
        ("f", None, "required argument is not a float", struct.error),
        ("d", None, "required argument is not a float", struct.error),
        ("f", 1e100, "value out of range", OverflowError),
        ("z", 13, "bad char in struct format", struct.error),
    ],
)
@pytest.mark.parametrize("col_name", [None, "value"])
def test_write_native_col_other_errors(native_writer, code, value, detail, cause, col_name):
    dest = bytearray(b"prefix")
    with pytest.raises(DataError) as exc:
        native_writer(code, [value], dest, col_name)
    col_msg = f" for column `{col_name}`" if col_name else ""
    assert str(exc.value) == f"Unable to create native array{col_msg}: {detail}"
    assert isinstance(exc.value.__cause__, cause)
    assert dest == b"prefix"


def test_write_native_col_preserves_type_error(native_writer):
    error = TypeError("index failed")

    class InvalidIndex:
        def __index__(self):
            raise error

    with pytest.raises(DataError) as exc:
        native_writer("i", [InvalidIndex()], bytearray(), "value")
    assert str(exc.value) == "Unable to create native array for column `value`: type mismatch (usually None in non-Nullable column)"
    assert exc.value.__cause__ is error


@pytest.mark.parametrize("code", "bBhHiIlLqQfd")
def test_write_native_col_success(native_writer, code):
    values = [13.5, 79.25] if code in "fd" else [13, 79]
    dest = bytearray(b"prefix")
    assert native_writer(code, values, dest, "value") == 0
    assert dest == b"prefix" + struct.pack(f"<2{code}", *values)
