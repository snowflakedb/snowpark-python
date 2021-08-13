#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All right reserved.
#

from snowflake.snowpark.types.sf_types import *


def test_integral_type():
    def verify_integral_type(tpe: DataType):
        assert isinstance(tpe, IntegralType)
        assert isinstance(tpe, NumericType)
        assert isinstance(tpe, AtomicType)
        assert isinstance(tpe, DataType)
        assert tpe.type_name == tpe.to_string
        assert tpe.type_name == tpe.__class__.__name__.replace("Type", "")

    for tpe in [ByteType(), ShortType(), IntegerType(), LongType()]:
        verify_integral_type(tpe)


def test_fractional_type():
    def verify_fractional_type(tpe: DataType):
        assert isinstance(tpe, FractionalType)
        assert isinstance(tpe, NumericType)
        assert isinstance(tpe, AtomicType)
        assert isinstance(tpe, DataType)
        assert tpe.type_name == tpe.to_string
        assert tpe.type_name == tpe.__class__.__name__.replace("Type", "")

    for tpe in [FloatType(), DoubleType()]:
        verify_fractional_type(tpe)


def test_decimal_type():
    tpe = DecimalType(38, 19)
    assert isinstance(tpe, FractionalType)
    assert isinstance(tpe, NumericType)
    assert isinstance(tpe, AtomicType)
    assert isinstance(tpe, DataType)
    assert tpe.type_name == tpe.to_string
    assert tpe.type_name == "Decimal(38,19)"


def test_string_type():
    tpe = StringType()
    assert isinstance(tpe, AtomicType)
    assert isinstance(tpe, DataType)
    assert tpe.type_name == tpe.to_string
    assert tpe.type_name == "String"


def test_boolean_type():
    tpe = BooleanType()
    assert isinstance(tpe, AtomicType)
    assert isinstance(tpe, DataType)
    assert tpe.type_name == tpe.to_string
    assert tpe.type_name == "Boolean"


def test_datetype_type():
    tpe = DateType()
    assert isinstance(tpe, AtomicType)
    assert isinstance(tpe, DataType)
    assert tpe.type_name == tpe.to_string
    assert tpe.type_name == "Date"


def test_binary_type():
    tpe = BinaryType()
    assert isinstance(tpe, AtomicType)
    assert isinstance(tpe, DataType)
    assert tpe.type_name == tpe.to_string
    assert tpe.type_name == "Binary"


def test_timestamp_type():
    tpe = TimestampType()
    assert isinstance(tpe, AtomicType)
    assert isinstance(tpe, DataType)
    assert tpe.type_name == tpe.to_string
    assert tpe.type_name == "Timestamp"


# Not in scala
def test_time_type():
    tpe = TimeType()
    assert isinstance(tpe, DataType)
    assert tpe.type_name == tpe.to_string
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
        tpe.to_string
        == "StructType[StructField(col1, Integer, Nullable=True), StructField(col2, String, Nullable=False)]"
    )

    assert tpe.fields[1] == StructField("col2", StringType(), nullable=False)
    # In scala, tpe is subscriptable and allows search by col-name
    assert tpe.fields[0] == StructField("col1", IntegerType())

    assert tpe.names == ["col1", "col2"]
