#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All right reserved.
#

# Many of the tests have been moved to unit/scala/test_datattype_suite.py
from decimal import Decimal

from snowflake.snowpark.functions import lit
from snowflake.snowpark.row import Row
from snowflake.snowpark.types.sf_types import (
    ArrayType,
    BinaryType,
    BooleanType,
    ByteType,
    DataType,
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    GeographyType,
    IntegerType,
    LongType,
    MapType,
    ShortType,
    StringType,
    StructField,
    StructType,
    TimestampType,
    TimeType,
    VariantType,
)


def test_verify_datatypes_reference(session):
    schema = StructType(
        [
            StructField("var", VariantType()),
            #StructField("geo", GeographyType()),
            StructField("date", DateType()),
            StructField("time", TimeType()),
            StructField("timestamp", TimestampType()),
            StructField("string", StringType()),
            StructField("boolean", BooleanType()),
            StructField("binary", BinaryType()),
            StructField("byte", ByteType()),
            StructField("short", ShortType()),
            StructField("int", IntegerType()),
            StructField("long", LongType()),
            StructField("float", FloatType()),
            StructField("double", DoubleType()),
            StructField("decimal", DecimalType(10, 2)),
            StructField("array", ArrayType(IntegerType())),
            StructField("map", MapType(ByteType(), TimeType())),
        ]
    )

    df = session.createDataFrame(
        [
            [
                None,
                #None,
                None,
                None,
                None,
                "a",
                True,
                None,
                1,
                2,
                3,
                4,
                5.0,
                6.0,
                Decimal(123),
                None,
                None,
            ]
        ],
        schema,
    )

    assert (
        str(df.schema.fields) == "[StructField(VAR, Variant, Nullable=True), "
        # "StructField(GEO, String, Nullable=True), "
        "StructField(DATE, Date, Nullable=True), "
        "StructField(TIME, Time, Nullable=True), "
        "StructField(TIMESTAMP, Timestamp, Nullable=True), "
        "StructField(STRING, String, Nullable=False), "
        "StructField(BOOLEAN, Boolean, Nullable=True), "
        "StructField(BINARY, Binary, Nullable=True), "
        "StructField(BYTE, Long, Nullable=False), "
        "StructField(SHORT, Long, Nullable=False), "
        "StructField(INT, Long, Nullable=False), "
        "StructField(LONG, Long, Nullable=False), "
        "StructField(FLOAT, Double, Nullable=False), "
        "StructField(DOUBLE, Double, Nullable=False), "
        "StructField(DECIMAL, Decimal(10,2), Nullable=False), "
        "StructField(ARRAY, ArrayType[String], Nullable=True), "
        "StructField(MAP, MapType[String,String], Nullable=True)]"
    )


def test_verify_datatypes_reference2(session):
    d1 = DecimalType(2, 1)
    d2 = DecimalType(2, 1)
    assert d1 == d2

    df = session.range(1).select(
        lit(0.05).cast(DecimalType(5, 2)).as_("a"),
        lit(0.07).cast(DecimalType(7, 2)).as_("b"),
    )

    assert df.collect() == [Row(Decimal("0.05"), Decimal("0.07"))]
    assert (
        str(df.schema.fields)
        == "[StructField(A, Decimal(5,2), Nullable=False), StructField(B, Decimal(7,2), Nullable=False)]"
    )
