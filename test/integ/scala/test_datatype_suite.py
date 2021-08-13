#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All right reserved.
#

# Many of the tests have been moved to unit/scala/test_datattype_suite.py
from decimal import Decimal

from snowflake.snowpark.types.sf_types import *


def test_verify_datatypes_reference(session_cnx):
    with session_cnx() as session:
        schema = StructType(
            [
                StructField("var", VariantType()),
                StructField("geo", GeographyType()),
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
                None,
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
        "StructField(GEO, String, Nullable=True), "
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
