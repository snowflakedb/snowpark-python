#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

from snowflake.snowpark.types import (
    BinaryType,
    BooleanType,
    ByteType,
    DataType,
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    ShortType,
    StringType,
    StructField,
    StructType,
    TimestampType,
    TimeType,
    _AtomicType,
    _FractionalType,
    _IntegralType,
    _NumericType,
)


def test_integral_type():
    def verify_integral_type(tpe: DataType):
        assert isinstance(tpe, _IntegralType)
        assert isinstance(tpe, _NumericType)
        assert isinstance(tpe, _AtomicType)
        assert isinstance(tpe, DataType)
        assert str(tpe) == f"{tpe.__class__.__name__}()"
        assert repr(tpe) == f"{tpe.__class__.__name__}()"

    for tpe in [ByteType(), ShortType(), IntegerType(), LongType()]:
        verify_integral_type(tpe)


def test_fractional_type():
    def verify_fractional_type(tpe: DataType):
        assert isinstance(tpe, _FractionalType)
        assert isinstance(tpe, _NumericType)
        assert isinstance(tpe, _AtomicType)
        assert isinstance(tpe, DataType)
        assert str(tpe) == f"{tpe.__class__.__name__}()"
        assert repr(tpe) == f"{tpe.__class__.__name__}()"

    for tpe in [FloatType(), DoubleType()]:
        verify_fractional_type(tpe)


def test_decimal_type():
    tpe = DecimalType(38, 19)
    assert isinstance(tpe, _FractionalType)
    assert isinstance(tpe, _NumericType)
    assert isinstance(tpe, _AtomicType)
    assert isinstance(tpe, DataType)
    assert str(tpe) == "DecimalType(38, 19)"
    assert repr(tpe) == "DecimalType(38, 19)"


def test_string_type():
    tpe = StringType(17)
    assert isinstance(tpe, _AtomicType)
    assert isinstance(tpe, DataType)
    assert str(tpe) == "StringType(17)"
    assert repr(tpe) == "StringType(17)"


def test_string_type_max():
    """
    Test that various edge cases with max length strings work as expected.
    Although contrived, it is possible that StringType objects will be compared that are created
    from sessions with different size limiations.
    """
    max_string = StringType()
    max_w_length_1 = StringType(16, True)
    max_w_length_2 = StringType(32, True)
    small_string = StringType(16)

    # Max strings all have the same repr
    assert all(
        repr(s) == "StringType()" for s in [max_string, max_w_length_1, max_w_length_2]
    )

    # Non-max strings include length in repr
    assert repr(small_string) == "StringType(16)"

    # Max string without defined length is equal to a max string with defined length
    assert max_string == max_w_length_1
    assert max_string == max_w_length_2

    # Max strings with defined lengths are not equal if their lengths are not equal
    assert max_w_length_1 != max_w_length_2

    # Strings of same length are equal regardless of if they are max length
    assert small_string == max_w_length_1


def test_boolean_type():
    tpe = BooleanType()
    assert isinstance(tpe, _AtomicType)
    assert isinstance(tpe, DataType)
    assert str(tpe) == "BooleanType()"
    assert repr(tpe) == "BooleanType()"


def test_datetype_type():
    tpe = DateType()
    assert isinstance(tpe, _AtomicType)
    assert isinstance(tpe, DataType)
    assert str(tpe) == "DateType()"
    assert repr(tpe) == "DateType()"


def test_binary_type():
    tpe = BinaryType()
    assert isinstance(tpe, _AtomicType)
    assert isinstance(tpe, DataType)
    assert str(tpe) == "BinaryType()"
    assert repr(tpe) == "BinaryType()"


def test_timestamp_type():
    tpe = TimestampType()
    assert isinstance(tpe, _AtomicType)
    assert isinstance(tpe, DataType)
    assert str(tpe) == "TimestampType()"
    assert repr(tpe) == "TimestampType()"


# Not in scala
def test_time_type():
    tpe = TimeType()
    assert isinstance(tpe, DataType)
    assert str(tpe) == "TimeType()"
    assert repr(tpe) == "TimeType()"


def test_structtype():
    tpe = StructType([])
    assert isinstance(tpe, DataType)
    assert len(tpe.fields) == 0

    tpe.fields.extend(
        [StructField("col1", IntegerType()), StructField("col2", StringType(11), False)]
    )
    assert len(tpe.fields) == 2
    assert (
        str(tpe)
        == "StructType([StructField('COL1', IntegerType(), nullable=True), StructField('COL2', StringType(11), nullable=False)])"
    )

    assert tpe.fields[1] == StructField("col2", StringType(11), nullable=False)
    # In scala, tpe is subscriptable and allows search by col-name
    assert tpe.fields[0] == StructField("col1", IntegerType(), nullable=True)

    assert tpe.names == ["COL1", "COL2"]
