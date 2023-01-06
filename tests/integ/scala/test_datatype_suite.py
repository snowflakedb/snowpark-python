#
# Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
#

# Many of the tests have been moved to unit/scala/test_datattype_suite.py
import datetime
import decimal
from decimal import Decimal

import pandas
from pandas.testing import assert_series_equal

from snowflake.snowpark import Row
from snowflake.snowpark.functions import lit
from snowflake.snowpark.types import (
    ArrayType,
    BinaryType,
    BooleanType,
    ByteType,
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

    df = session.create_dataframe(
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
        str(df.schema.fields) == "[StructField('VAR', VariantType(), nullable=True), "
        "StructField('GEO', GeographyType(), nullable=True), "
        "StructField('DATE', DateType(), nullable=True), "
        "StructField('TIME', TimeType(), nullable=True), "
        "StructField('TIMESTAMP', TimestampType(), nullable=True), "
        "StructField('STRING', StringType(), nullable=False), "
        "StructField('BOOLEAN', BooleanType(), nullable=True), "
        "StructField('BINARY', BinaryType(), nullable=True), "
        "StructField('BYTE', LongType(), nullable=False), "
        "StructField('SHORT', LongType(), nullable=False), "
        "StructField('INT', LongType(), nullable=False), "
        "StructField('LONG', LongType(), nullable=False), "
        "StructField('FLOAT', DoubleType(), nullable=False), "
        "StructField('DOUBLE', DoubleType(), nullable=False), "
        "StructField('DECIMAL', DecimalType(10, 2), nullable=False), "
        "StructField('ARRAY', ArrayType(StringType()), nullable=True), "
        "StructField('MAP', MapType(StringType(), StringType()), nullable=True)]"
    )
    assert_series_equal(
        df.dtypes,
        pandas.Series(
            [
                dict,
                dict,
                datetime.date,
                datetime.time,
                datetime.datetime,
                str,
                bool,
                bytes,
                int,
                int,
                int,
                int,
                float,
                float,
                decimal.Decimal,
                list,
                dict,
            ],
            index=[
                "VAR",
                "GEO",
                "DATE",
                "TIME",
                "TIMESTAMP",
                "STRING",
                "BOOLEAN",
                "BINARY",
                "BYTE",
                "SHORT",
                "INT",
                "LONG",
                "FLOAT",
                "DOUBLE",
                "DECIMAL",
                "ARRAY",
                "MAP",
            ],
        ),
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
        == "[StructField('A', DecimalType(5, 2), nullable=False), "
        "StructField('B', DecimalType(7, 2), nullable=False)]"
    )
