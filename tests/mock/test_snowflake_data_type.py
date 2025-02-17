#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import pytest

from snowflake.snowpark.mock._snowflake_data_type import (
    ColumnType,
    coerce_t1_into_t2,
    get_coerce_result_type,
)
from snowflake.snowpark.types import (
    ArrayType,
    BinaryType,
    BooleanType,
    ByteType,
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    MapType,
    NullType,
    ShortType,
    StringType,
    StructType,
    TimestampType,
    TimeType,
    VariantType,
)


@pytest.mark.parametrize(
    "t1,t2,expected",
    [
        (ArrayType, ArrayType, ArrayType),
        (ArrayType, BinaryType, None),
        (ArrayType, BooleanType, None),
        (ArrayType, ByteType, None),
        (ArrayType, DateType, None),
        (ArrayType, DecimalType, None),
        (ArrayType, DoubleType, None),
        (ArrayType, FloatType, None),
        (ArrayType, IntegerType, None),
        (ArrayType, LongType, None),
        (ArrayType, MapType, None),
        (ArrayType, NullType, None),
        (ArrayType, ShortType, None),
        (ArrayType, StringType, None),
        (ArrayType, StructType, None),
        (ArrayType, TimeType, None),
        (ArrayType, TimestampType, None),
        (ArrayType, VariantType, VariantType),
        (BinaryType, ArrayType, None),
        (BinaryType, BinaryType, BinaryType),
        (BinaryType, BooleanType, None),
        (BinaryType, ByteType, None),
        (BinaryType, DateType, None),
        (BinaryType, DecimalType, None),
        (BinaryType, DoubleType, None),
        (BinaryType, FloatType, None),
        (BinaryType, IntegerType, None),
        (BinaryType, LongType, None),
        (BinaryType, MapType, None),
        (BinaryType, NullType, None),
        (BinaryType, ShortType, None),
        (BinaryType, StringType, None),
        (BinaryType, StructType, None),
        (BinaryType, TimeType, None),
        (BinaryType, TimestampType, None),
        (BinaryType, VariantType, None),
        (BooleanType, ArrayType, None),
        (BooleanType, BinaryType, None),
        (BooleanType, BooleanType, BooleanType),
        (BooleanType, ByteType, None),
        (BooleanType, DateType, None),
        (BooleanType, DecimalType, None),
        (BooleanType, DoubleType, None),
        (BooleanType, FloatType, None),
        (BooleanType, IntegerType, None),
        (BooleanType, LongType, None),
        (BooleanType, MapType, None),
        (BooleanType, NullType, None),
        (BooleanType, ShortType, None),
        (BooleanType, StringType, StringType),
        (BooleanType, StructType, None),
        (BooleanType, TimeType, None),
        (BooleanType, TimestampType, None),
        (BooleanType, VariantType, VariantType),
        (ByteType, ArrayType, None),
        (ByteType, BinaryType, None),
        (ByteType, BooleanType, BooleanType),
        (ByteType, ByteType, ByteType),
        (ByteType, DateType, None),
        (ByteType, DecimalType, DecimalType),
        (ByteType, DoubleType, DoubleType),
        (ByteType, FloatType, FloatType),
        (ByteType, IntegerType, LongType),
        (ByteType, LongType, LongType),
        (ByteType, MapType, None),
        (ByteType, NullType, None),
        (ByteType, ShortType, LongType),
        (ByteType, StringType, None),
        (ByteType, StructType, None),
        (ByteType, TimeType, None),
        (ByteType, TimestampType, None),
        (ByteType, VariantType, VariantType),
        (DateType, ArrayType, None),
        (DateType, BinaryType, None),
        (DateType, BooleanType, None),
        (DateType, ByteType, None),
        (DateType, DateType, DateType),
        (DateType, DecimalType, None),
        (DateType, DoubleType, None),
        (DateType, FloatType, None),
        (DateType, IntegerType, None),
        (DateType, LongType, None),
        (DateType, MapType, None),
        (DateType, NullType, None),
        (DateType, ShortType, None),
        (DateType, StringType, None),
        (DateType, StructType, None),
        (DateType, TimeType, None),
        (DateType, TimestampType, TimestampType),
        (DateType, VariantType, VariantType),
        (DecimalType, ArrayType, None),
        (DecimalType, BinaryType, None),
        (DecimalType, BooleanType, BooleanType),
        (DecimalType, ByteType, None),
        (DecimalType, DateType, None),
        (DecimalType, DecimalType, DecimalType),
        (DecimalType, DoubleType, DoubleType),
        (DecimalType, FloatType, DoubleType),
        (DecimalType, IntegerType, None),
        (DecimalType, LongType, None),
        (DecimalType, MapType, None),
        (DecimalType, NullType, None),
        (DecimalType, ShortType, None),
        (DecimalType, StringType, None),
        (DecimalType, StructType, None),
        (DecimalType, TimeType, None),
        (DecimalType, TimestampType, None),
        (DecimalType, VariantType, VariantType),
        (DoubleType, ArrayType, None),
        (DoubleType, BinaryType, None),
        (DoubleType, BooleanType, BooleanType),
        (DoubleType, ByteType, None),
        (DoubleType, DateType, None),
        (DoubleType, DecimalType, DoubleType),
        (DoubleType, DoubleType, DoubleType),
        (DoubleType, FloatType, DoubleType),
        (DoubleType, IntegerType, None),
        (DoubleType, LongType, None),
        (DoubleType, MapType, None),
        (DoubleType, NullType, None),
        (DoubleType, ShortType, None),
        (DoubleType, StringType, None),
        (DoubleType, StructType, None),
        (DoubleType, TimeType, None),
        (DoubleType, TimestampType, None),
        (DoubleType, VariantType, VariantType),
        (FloatType, ArrayType, None),
        (FloatType, BinaryType, None),
        (FloatType, BooleanType, BooleanType),
        (FloatType, ByteType, None),
        (FloatType, DateType, None),
        (FloatType, DecimalType, DoubleType),
        (FloatType, DoubleType, DoubleType),
        (FloatType, FloatType, FloatType),
        (FloatType, IntegerType, None),
        (FloatType, LongType, None),
        (FloatType, MapType, None),
        (FloatType, NullType, None),
        (FloatType, ShortType, None),
        (FloatType, StringType, None),
        (FloatType, StructType, None),
        (FloatType, TimeType, None),
        (FloatType, TimestampType, None),
        (FloatType, VariantType, VariantType),
        (IntegerType, ArrayType, None),
        (IntegerType, BinaryType, None),
        (IntegerType, BooleanType, BooleanType),
        (IntegerType, ByteType, LongType),
        (IntegerType, DateType, None),
        (IntegerType, DecimalType, DecimalType),
        (IntegerType, DoubleType, DoubleType),
        (IntegerType, FloatType, FloatType),
        (IntegerType, IntegerType, IntegerType),
        (IntegerType, LongType, LongType),
        (IntegerType, MapType, None),
        (IntegerType, NullType, None),
        (IntegerType, ShortType, LongType),
        (IntegerType, StringType, None),
        (IntegerType, StructType, None),
        (IntegerType, TimeType, None),
        (IntegerType, TimestampType, None),
        (IntegerType, VariantType, VariantType),
        (LongType, ArrayType, None),
        (LongType, BinaryType, None),
        (LongType, BooleanType, BooleanType),
        (LongType, ByteType, LongType),
        (LongType, DateType, None),
        (LongType, DecimalType, DecimalType),
        (LongType, DoubleType, DoubleType),
        (LongType, FloatType, FloatType),
        (LongType, IntegerType, LongType),
        (LongType, LongType, LongType),
        (LongType, MapType, None),
        (LongType, NullType, None),
        (LongType, ShortType, LongType),
        (LongType, StringType, None),
        (LongType, StructType, None),
        (LongType, TimeType, None),
        (LongType, TimestampType, None),
        (LongType, VariantType, VariantType),
        (MapType, ArrayType, None),
        (MapType, BinaryType, None),
        (MapType, BooleanType, None),
        (MapType, ByteType, None),
        (MapType, DateType, None),
        (MapType, DecimalType, None),
        (MapType, DoubleType, None),
        (MapType, FloatType, None),
        (MapType, IntegerType, None),
        (MapType, LongType, None),
        (MapType, MapType, MapType),
        (MapType, NullType, None),
        (MapType, ShortType, None),
        (MapType, StringType, None),
        (MapType, StructType, None),
        (MapType, TimeType, None),
        (MapType, TimestampType, None),
        (MapType, VariantType, VariantType),
        (NullType, ArrayType, ArrayType),
        (NullType, BinaryType, BinaryType),
        (NullType, BooleanType, BooleanType),
        (NullType, ByteType, ByteType),
        (NullType, DateType, DateType),
        (NullType, DecimalType, DecimalType),
        (NullType, DoubleType, DoubleType),
        (NullType, FloatType, FloatType),
        (NullType, IntegerType, IntegerType),
        (NullType, LongType, LongType),
        (NullType, MapType, MapType),
        (NullType, NullType, NullType),
        (NullType, ShortType, ShortType),
        (NullType, StringType, StringType),
        (NullType, StructType, StructType),
        (NullType, TimeType, TimeType),
        (NullType, TimestampType, TimestampType),
        (NullType, VariantType, VariantType),
        (ShortType, ArrayType, None),
        (ShortType, BinaryType, None),
        (ShortType, BooleanType, BooleanType),
        (ShortType, ByteType, LongType),
        (ShortType, DateType, None),
        (ShortType, DecimalType, DecimalType),
        (ShortType, DoubleType, DoubleType),
        (ShortType, FloatType, FloatType),
        (ShortType, IntegerType, LongType),
        (ShortType, LongType, LongType),
        (ShortType, MapType, None),
        (ShortType, NullType, None),
        (ShortType, ShortType, ShortType),
        (ShortType, StringType, None),
        (ShortType, StructType, None),
        (ShortType, TimeType, None),
        (ShortType, TimestampType, None),
        (ShortType, VariantType, VariantType),
        (StringType, ArrayType, None),
        (StringType, BinaryType, None),
        (StringType, BooleanType, None),
        (StringType, ByteType, ByteType),
        (StringType, DateType, DateType),
        (StringType, DecimalType, DecimalType),
        (StringType, DoubleType, DoubleType),
        (StringType, FloatType, FloatType),
        (StringType, IntegerType, IntegerType),
        (StringType, LongType, LongType),
        (StringType, MapType, None),
        (StringType, NullType, None),
        (StringType, ShortType, ShortType),
        (StringType, StringType, StringType),
        (StringType, StructType, None),
        (StringType, TimeType, TimeType),
        (StringType, TimestampType, TimestampType),
        (StringType, VariantType, VariantType),
        (StructType, ArrayType, None),
        (StructType, BinaryType, None),
        (StructType, BooleanType, None),
        (StructType, ByteType, None),
        (StructType, DateType, None),
        (StructType, DecimalType, None),
        (StructType, DoubleType, None),
        (StructType, FloatType, None),
        (StructType, IntegerType, None),
        (StructType, LongType, None),
        (StructType, MapType, None),
        (StructType, NullType, None),
        (StructType, ShortType, None),
        (StructType, StringType, None),
        (StructType, StructType, StructType),
        (StructType, TimeType, None),
        (StructType, TimestampType, None),
        (StructType, VariantType, None),
        (TimeType, ArrayType, None),
        (TimeType, BinaryType, None),
        (TimeType, BooleanType, None),
        (TimeType, ByteType, None),
        (TimeType, DateType, None),
        (TimeType, DecimalType, None),
        (TimeType, DoubleType, None),
        (TimeType, FloatType, None),
        (TimeType, IntegerType, None),
        (TimeType, LongType, None),
        (TimeType, MapType, None),
        (TimeType, NullType, None),
        (TimeType, ShortType, None),
        (TimeType, StringType, None),
        (TimeType, StructType, None),
        (TimeType, TimeType, TimeType),
        (TimeType, TimestampType, None),
        (TimeType, VariantType, VariantType),
        (TimestampType, ArrayType, None),
        (TimestampType, BinaryType, None),
        (TimestampType, BooleanType, None),
        (TimestampType, ByteType, None),
        (TimestampType, DateType, None),
        (TimestampType, DecimalType, None),
        (TimestampType, DoubleType, None),
        (TimestampType, FloatType, None),
        (TimestampType, IntegerType, None),
        (TimestampType, LongType, None),
        (TimestampType, MapType, None),
        (TimestampType, NullType, None),
        (TimestampType, ShortType, None),
        (TimestampType, StringType, None),
        (TimestampType, StructType, None),
        (TimestampType, TimeType, None),
        (TimestampType, TimestampType, TimestampType),
        (TimestampType, VariantType, VariantType),
        (VariantType, ArrayType, None),
        (VariantType, BinaryType, None),
        (VariantType, BooleanType, None),
        (VariantType, ByteType, None),
        (VariantType, DateType, None),
        (VariantType, DecimalType, None),
        (VariantType, DoubleType, None),
        (VariantType, FloatType, None),
        (VariantType, IntegerType, None),
        (VariantType, LongType, None),
        (VariantType, MapType, None),
        (VariantType, NullType, None),
        (VariantType, ShortType, None),
        (VariantType, StringType, None),
        (VariantType, StructType, None),
        (VariantType, TimeType, None),
        (VariantType, TimestampType, None),
        (VariantType, VariantType, VariantType),
    ],
)
def test_coerce_t1_into_t2_defaults(t1, t2, expected):
    e = expected() if expected is not None else None
    actual = coerce_t1_into_t2(t1(), t2())
    assert (
        actual == e
    ), f"expected {t1()} coerced into {t2()} to be {e}, but got {actual} instead"


def test_coerce_t1_into_t2_strings():
    assert coerce_t1_into_t2(StringType(1), StringType(5)) == StringType(5)


SMALL, LARGE = (1, 38)
SMALL_SMALL = (SMALL, SMALL)
SMALL_LARGE = (SMALL, LARGE)
LARGE_SMALL = (LARGE, SMALL)
LARGE_LARGE = (LARGE, LARGE)


@pytest.mark.parametrize(
    "p1,p2,expected",
    [
        (LARGE_LARGE, LARGE_SMALL, LARGE_LARGE),
        (LARGE_LARGE, SMALL_LARGE, LARGE_LARGE),
        (LARGE_LARGE, SMALL_SMALL, LARGE_LARGE),
        (LARGE_SMALL, LARGE_LARGE, LARGE_LARGE),
        (LARGE_SMALL, SMALL_LARGE, LARGE_LARGE),
        (LARGE_SMALL, SMALL_SMALL, LARGE_SMALL),
        (SMALL_LARGE, LARGE_LARGE, LARGE_LARGE),
        (SMALL_LARGE, LARGE_SMALL, LARGE_LARGE),
        (SMALL_LARGE, SMALL_SMALL, LARGE_LARGE),
        (SMALL_SMALL, LARGE_LARGE, LARGE_LARGE),
        (SMALL_SMALL, LARGE_SMALL, LARGE_SMALL),
        (SMALL_SMALL, SMALL_LARGE, LARGE_LARGE),
    ],
)
def test_coerce_t1_into_t2_decimals(p1, p2, expected):
    t1 = DecimalType(*p1)
    t2 = DecimalType(*p2)
    expected = DecimalType(*expected)
    actual = coerce_t1_into_t2(t1, t2)
    assert (
        expected == actual
    ), f"expected {t1} coerced into {t2} to be {expected}, but got {actual} instead"


def test_coerce_t1_into_t2_semi_structured():
    m1 = MapType(StringType(), StringType())
    m2 = MapType(StringType(), IntegerType())
    structured_m = MapType(StringType(), StringType())
    mv = MapType(VariantType(), VariantType())

    assert coerce_t1_into_t2(m1, m1) == m1
    assert coerce_t1_into_t2(m2, m2) == m2
    assert coerce_t1_into_t2(m1, structured_m) == structured_m
    assert coerce_t1_into_t2(m1, m2) == mv

    a1 = ArrayType(StringType())
    a2 = ArrayType(IntegerType())
    structured_a = ArrayType(StringType(), structured=True)
    av = ArrayType(VariantType())

    assert coerce_t1_into_t2(a1, a1) == a1
    assert coerce_t1_into_t2(a1, structured_a) == structured_a
    assert coerce_t1_into_t2(a2, a2) == a2
    assert coerce_t1_into_t2(a1, a2) == av


def test_get_coerce_result_type_neg():
    # Incompatible types will not have a coerce result
    c1 = ColumnType(BinaryType(), True)
    c2 = ColumnType(BooleanType(), True)
    assert get_coerce_result_type(c1, c2) is None
