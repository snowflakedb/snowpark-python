#
# Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
#
import datetime
import decimal
import time

import pytest

from snowflake.snowpark import Row
from snowflake.snowpark.functions import col
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


def test_limit_on_order_by(session, is_sample_data_available):
    # Tests using SNOWFLAKE_SAMPLE_DATA, it may be not available on some test deployments
    if not is_sample_data_available:
        pytest.skip("SNOWFLAKE_SAMPLE_DATA is not available in this deployment")

    a = (
        session.table("SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.LINEITEM")
        .select("L_RETURNFLAG", "L_SHIPMODE")
        .filter(col("L_RETURNFLAG") == "A")
        .group_by("L_RETURNFLAG", "L_SHIPMODE")
        .count()
    )
    n = (
        session.table("SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.LINEITEM")
        .select("L_RETURNFLAG", "L_SHIPMODE")
        .filter(col("L_RETURNFLAG") == "N")
        .group_by("L_RETURNFLAG", "L_SHIPMODE")
        .count()
    )

    union = a.union_all(n)
    result = union.select(col("COUNT")).sort(col("COUNT")).limit(10).collect()
    for e1, e2 in zip(result[:-1], result[1:]):
        assert int(e1[0]) < int(e2[0])


def test_create_dataframe_for_large_values_check_plan(session):
    def check_plan(df, data):
        assert df._plan.queries[0].sql.strip().startswith("CREATE  TEMPORARY  TABLE")
        assert df._plan.queries[1].sql.strip().startswith("INSERT  INTO")
        assert df._plan.queries[2].sql.strip().startswith("SELECT")
        assert len(df._plan.post_actions) == 1
        assert df.sort("id").collect() == data

    large_data = [Row(i) for i in range(1025)]
    schema = StructType([StructField("ID", LongType())])
    df1 = session.create_dataframe(large_data, schema)
    df2 = session.create_dataframe(large_data).to_df("id")
    check_plan(df1, large_data)
    check_plan(df2, large_data)


def test_create_dataframe_for_large_values_basic_types(session):
    schema = StructType(
        [
            StructField("ID", LongType()),
            StructField("string", StringType()),
            StructField("byte", ByteType()),
            StructField("short", ShortType()),
            StructField("int", IntegerType()),
            StructField("long", LongType()),
            StructField("float", FloatType()),
            StructField("double", DoubleType()),
            StructField("decimal", DecimalType(10, 3)),
            StructField("boolean", BooleanType()),
            StructField("binary", BinaryType()),
            StructField("timestamp", TimestampType()),
            StructField("time", TimeType()),
            StructField("date", DateType()),
        ]
    )
    row_count = 1024
    large_data = [
        Row(
            i,
            "a",
            1,
            2,
            3,
            4,
            1.1,
            1.2,
            decimal.Decimal(0.5),
            True,
            bytearray([1, 2]),
            datetime.datetime.strptime(
                "2017-02-24 12:00:05.456", "%Y-%m-%d %H:%M:%S.%f"
            ),
            datetime.datetime.strptime("20:57:06", "%H:%M:%S").time(),
            datetime.datetime.strptime("2017-02-25", "%Y-%m-%d").date(),
        )
        for i in range(row_count)
    ]
    large_data.append(Row(row_count, *([None] * (len(large_data[0]) - 1))))
    df = session.create_dataframe(large_data, schema)
    assert [type(field.datatype) for field in df.schema.fields] == [
        LongType,
        StringType,
        LongType,
        LongType,
        LongType,
        LongType,
        DoubleType,
        DoubleType,
        DecimalType,
        BooleanType,
        BinaryType,
        TimestampType,
        TimeType,
        DateType,
    ]
    assert df.sort("id").collect() == large_data


def test_create_dataframe_for_large_values_array_map_variant(session):
    schema = StructType(
        [
            StructField("id", LongType()),
            StructField("array", ArrayType(None)),
            StructField("map", MapType(None, None)),
            StructField("variant", VariantType()),
            StructField("geography", GeographyType()),
        ]
    )

    row_count = 350
    large_data = [
        Row(i, ["'", 2], {"'": 1}, {"a": "foo"}, "POINT(30 10)")
        for i in range(row_count)
    ]
    large_data.append(Row(row_count, None, None, None, None))
    df = session.create_dataframe(large_data, schema)
    assert [type(field.datatype) for field in df.schema.fields] == [
        LongType,
        ArrayType,
        MapType,
        VariantType,
        GeographyType,
    ]
    geography_string = """\
{
  "coordinates": [
    30,
    10
  ],
  "type": "Point"
}"""
    expected = [
        Row(
            i,
            '[\n  "\'",\n  2\n]',
            '{\n  "\'": 1\n}',
            '{\n  "a": "foo"\n}',
            geography_string,
        )
        for i in range(row_count)
    ]
    expected.append(Row(row_count, None, None, None, None))
    assert df.sort("id").collect() == expected
