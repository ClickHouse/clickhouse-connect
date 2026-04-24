import pytest
from sqlalchemy import DateTime, Integer
from sqlalchemy.exc import ArgumentError

from clickhouse_connect.cc_sqlalchemy.datatypes.base import sqla_type_from_name, sqla_type_map
from clickhouse_connect.cc_sqlalchemy.datatypes.sqltypes import (
    UUID,
    DateTime64,
    Int64,
    LowCardinality,
    Nullable,
    QBit,
    String,
    Tuple,
    UInt32,
    UInt64,
)
from clickhouse_connect.cc_sqlalchemy.datatypes.sqltypes import DateTime as ChDateTime


def test_mapping():
    assert issubclass(sqla_type_map["UInt64"], Integer)
    assert issubclass(sqla_type_map["DateTime"], DateTime)


def test_sqla():
    int16 = sqla_type_from_name("Int16")
    assert "Int16" == int16._compiler_dispatch(None)
    enum = sqla_type_from_name("Enum8('value1' = 7, 'value2'=5)")
    assert "Enum8('value2' = 5, 'value1' = 7)" == enum._compiler_dispatch(None)


def test_nullable():
    nullable = Nullable(Int64)
    assert nullable.__class__ == Int64
    nullable = Nullable(DateTime64(6))
    assert nullable.__class__ == DateTime64
    assert nullable.name == "Nullable(DateTime64(6))"


def test_low_cardinality():
    lc_str = LowCardinality(Nullable(String))
    assert lc_str.__class__ == String
    assert lc_str.name == "LowCardinality(Nullable(String))"


def test_qbit():
    qbit = sqla_type_from_name("QBit(Float32, 768)")
    assert qbit.__class__ == QBit
    assert qbit.name == "QBit(Float32, 768)"
    assert qbit._compiler_dispatch(None) == "QBit(Float32, 768)"

    qbit2 = QBit("Float32", 768)
    assert qbit2.name == "QBit(Float32, 768)"

    qbit_bf16 = sqla_type_from_name("QBit(BFloat16, 128)")
    assert qbit_bf16.name == "QBit(BFloat16, 128)"

    qbit_f64 = sqla_type_from_name("QBit(Float64, 1536)")
    assert qbit_f64.name == "QBit(Float64, 1536)"


def test_datetime_timezone_alias():
    assert ChDateTime(timezone="UTC").name == ChDateTime(tz="UTC").name


def test_datetime64_timezone_alias():
    assert DateTime64(3, timezone="America/New_York").name == DateTime64(3, tz="America/New_York").name


def test_datetime_both_tz_and_timezone_raises():
    with pytest.raises(ArgumentError):
        ChDateTime(tz="UTC", timezone="UTC")
    with pytest.raises(ArgumentError):
        DateTime64(3, tz="UTC", timezone="UTC")


def test_datetime_timezone_true_raises():
    with pytest.raises(ArgumentError) as exc_info:
        ChDateTime(timezone=True)
    assert "zone" in str(exc_info.value).lower()
    with pytest.raises(ArgumentError) as exc_info:
        DateTime64(3, timezone=True)
    assert "zone" in str(exc_info.value).lower()


def test_datetime_timezone_false_is_noop():
    """
    SQLAlchemy's type-adaptation machinery passes timezone=False when cloning DateTime types
    (inherited from SqlaDateTime.timezone default). We silently accept it as equivalent to no
    zone rather than raising, so our type survives SA's internal copy/adapt calls.
    """
    assert ChDateTime(timezone=False).name == ChDateTime().name
    assert DateTime64(3, timezone=False).name == DateTime64(3).name
    # tz= still wins if passed alongside timezone=False
    assert ChDateTime(tz="UTC", timezone=False).name == ChDateTime(tz="UTC").name


def test_tuple_variadic():
    assert Tuple(UInt32, UInt64).name == Tuple(elements=[UInt32, UInt64]).name


def test_tuple_variadic_single():
    tup = Tuple(UInt32)
    assert tup.name == Tuple(elements=[UInt32]).name


def test_tuple_variadic_with_uuid():
    assert Tuple(UInt32, UUID, UInt64).name == Tuple(elements=[UInt32, UUID, UInt64]).name


def test_tuple_both_positional_and_kwarg_raises():
    with pytest.raises(ArgumentError):
        Tuple(UInt32, elements=[UInt64])


def test_tuple_zero_args_does_not_crash():
    """
    Regression: SQLAlchemy's dialect_impl -> adapt -> constructor_copy path
    can call Tuple() with no arguments (get_cls_kwargs can't introspect
    keyword-only args that follow *args, so it returns an empty set). The
    constructor must accept that cleanly instead of iterating None.
    """
    # Must not raise.
    Tuple()


def test_tuple_adapt_preserves_type_def():
    """
    Regression: SQLAlchemy's dialect_impl calls adapt() to get a dialect-local
    impl instance. The adapted instance must carry the same type_def so the
    rendered type (used elsewhere in compilation and result mapping) matches
    the source. Our adapt override copies state via __dict__ since SA's
    default constructor_copy can't see our signature's keyword-only args.
    """
    source = Tuple(UInt32, UInt64)
    adapted = source.adapt(type(source))
    assert adapted.type_def == source.type_def
    assert adapted.name == source.name
