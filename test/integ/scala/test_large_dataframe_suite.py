#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All right reserved.
#
import datetime
import decimal

from snowflake.snowpark.row import Row
from snowflake.snowpark.types.sf_types import (
    ArrayType,
    BinaryType,
    BooleanType,
    ByteType,
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    LongType,
    MapType,
    StringType,
    StructField,
    StructType,
    TimestampType,
    TimeType,
    VariantType,
)


def test_create_dataframe_for_large_values_check_plan(session):
    large_data = [Row(i) for i in range(1025)]
    df = session.createDataFrame(large_data).toDF("id")
    assert (
        df._DataFrame__plan.queries[0]
        .sql.strip()
        .startswith("CREATE  TEMPORARY  TABLE")
    )
    assert df._DataFrame__plan.queries[1].sql.strip().startswith("INSERT  INTO")
    assert df._DataFrame__plan.queries[2].sql.strip().startswith("SELECT")
    assert len(df._DataFrame__plan.post_actions) == 1
    assert df.sort("id").collect() == large_data


def test_create_dataframe_for_large_values_basic_types(session):
    schema = StructType(
        [
            StructField("ID", LongType()),
            StructField("string", StringType()),
            StructField("byte", ByteType()),
            StructField("float", FloatType()),
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
                1.1,
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
    df = session.createDataFrame(large_data, schema)
    assert [type(field.datatype) for field in df.schema.fields] == [
        LongType,
        StringType,
        LongType,
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
        ]
    )

    row_count = 350
    large_data = [Row(i, ["'", 2], {"'": 1}, {"a": "foo"}) for i in range(row_count)]
    large_data.append(Row(row_count, None, None, None))
    df = session.createDataFrame(large_data, schema)
    assert [type(field.datatype) for field in df.schema.fields] == [
        LongType,
        ArrayType,
        MapType,
        VariantType,
    ]
    expected = [
        Row(i, '[\n  "\'",\n  2\n]', '{\n  "\'": 1\n}', '{\n  "a": "foo"\n}')
        for i in range(row_count)
    ]
    expected.append(Row(row_count, None, None, None))
    assert df.sort("id").collect() == expected
