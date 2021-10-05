#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
#

from snowflake.snowpark.types import (
    _AtomicType,
    BinaryType,
    BooleanType,
    ByteType,
    DataType,
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    _FractionalType,
    IntegerType,
    _IntegralType,
    LongType,
    _NumericType,
    ShortType,
    StringType,
    StructField,
    StructType,
    TimestampType,
    TimeType,
)


def test_integral_type():
    def verify_integral_type(tpe: DataType):
        assert isinstance(tpe, _IntegralType)
        assert isinstance(tpe, _NumericType)
        assert isinstance(tpe, _AtomicType)
        assert isinstance(tpe, DataType)
        assert tpe.type_name == str(tpe)
        assert tpe.type_name == tpe.__class__.__name__.replace("Type", "")

    for tpe in [ByteType(), ShortType(), IntegerType(), LongType()]:
        verify_integral_type(tpe)


def test_fractional_type():
    def verify_fractional_type(tpe: DataType):
        assert isinstance(tpe, _FractionalType)
        assert isinstance(tpe, _NumericType)
        assert isinstance(tpe, _AtomicType)
        assert isinstance(tpe, DataType)
        assert tpe.type_name == str(tpe)
        assert tpe.type_name == tpe.__class__.__name__.replace("Type", "")

    for tpe in [FloatType(), DoubleType()]:
        verify_fractional_type(tpe)


def test_decimal_type():
    tpe = DecimalType(38, 19)
    assert isinstance(tpe, _FractionalType)
    assert isinstance(tpe, _NumericType)
    assert isinstance(tpe, _AtomicType)
    assert isinstance(tpe, DataType)
    assert tpe.type_name == str(tpe)
    assert tpe.type_name == "Decimal(38, 19)"


def test_string_type():
    tpe = StringType()
    assert isinstance(tpe, _AtomicType)
    assert isinstance(tpe, DataType)
    assert tpe.type_name == str(tpe)
    assert tpe.type_name == "String"


def test_boolean_type():
    tpe = BooleanType()
    assert isinstance(tpe, _AtomicType)
    assert isinstance(tpe, DataType)
    assert tpe.type_name == str(tpe)
    assert tpe.type_name == "Boolean"


def test_datetype_type():
    tpe = DateType()
    assert isinstance(tpe, _AtomicType)
    assert isinstance(tpe, DataType)
    assert tpe.type_name == str(tpe)
    assert tpe.type_name == "Date"


def test_binary_type():
    tpe = BinaryType()
    assert isinstance(tpe, _AtomicType)
    assert isinstance(tpe, DataType)
    assert tpe.type_name == str(tpe)
    assert tpe.type_name == "Binary"


def test_timestamp_type():
    tpe = TimestampType()
    assert isinstance(tpe, _AtomicType)
    assert isinstance(tpe, DataType)
    assert tpe.type_name == str(tpe)
    assert tpe.type_name == "Timestamp"


# Not in scala
def test_time_type():
    tpe = TimeType()
    assert isinstance(tpe, DataType)
    assert tpe.type_name == str(tpe)
    assert tpe.type_name == "Time"


def test_structtype():
    tpe = StructType([])
    assert isinstance(tpe, DataType)
    assert len(tpe.fields) == 0

    tpe.fields.extend(
        [StructField("col1", IntegerType()), StructField("col2", StringType(), False)]
    )
    assert len(tpe.fields) == 2
    assert tpe.type_name == "Struct"
    assert (
        str(tpe)
        == "StructType[StructField(col1, Integer, Nullable=True), StructField(col2, String, Nullable=False)]"
    )

    assert tpe.fields[1] == StructField("col2", StringType(), nullable=False)
    # In scala, tpe is subscriptable and allows search by col-name
    assert tpe.fields[0] == StructField("col1", IntegerType())

    assert tpe.names == ["col1", "col2"]
