#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import datetime
from decimal import Decimal

import pytz

from snowflake.snowpark.types import (
    StructType,
    StructField,
    LongType,
    StringType,
    BooleanType,
    BinaryType,
    DateType,
    TimestampType,
    DecimalType,
    DoubleType,
    TimestampTimeZone,
    VariantType,
)

TEST_TABLE_NAME = "ALL_TYPE_TABLE_2"  # ALL_TYPE_TABLE_2 contains None data while ALL_TYPE_TABLE doesn't
TZ_INFO = pytz.timezone("America/Los_Angeles")
EXPECTED_TEST_DATA = [
    tuple([None] * 18),
    (
        -113,
        -14623,
        74665,
        7633120703,
        41.216453552246094,
        1063.0827475381134,
        Decimal("9960.28"),
        "str_7962",
        True,
        bytearray(b"\xad\xa9\xdd\xa2"),
        datetime.date(2025, 4, 27),
        TZ_INFO.localize(
            datetime.datetime(2025, 4, 16, 10, 42, 48, 565000), is_dst=True
        ),
        datetime.datetime(2025, 4, 16, 17, 51, 18, 565000),
        "[\n  6,\n  87\n]",
        '{\n  "key1": 83,\n  "key2": 12\n}',
        '{\n  "field1": "f_77",\n  "field2": 13\n}',
        "1-8",
        "0 23:14:09.000000000",
    ),
    (
        -96,
        -431,
        78281,
        2300077013,
        84.57820892333984,
        8415.70918243513,
        Decimal("1669.93"),
        "str_8208",
        False,
        bytearray(b"\xad\xa9\xdd\xa2"),
        datetime.date(2025, 7, 14),
        TZ_INFO.localize(
            datetime.datetime(2025, 4, 16, 10, 40, 11, 565000), is_dst=True
        ),
        datetime.datetime(2025, 4, 16, 17, 42, 39, 565000),
        "[\n  0,\n  89\n]",
        '{\n  "key1": 97,\n  "key2": 33\n}',
        '{\n  "field1": "f_84",\n  "field2": 1\n}',
        "1-10",
        "2 11:12:05.000000000",
    ),
    (
        -34,
        25393,
        35234,
        5644171805,
        18.264881134033203,
        9187.446999674603,
        Decimal("269.89"),
        "str_8541",
        True,
        bytearray(b"\xad\xa9\xdd\xa2"),
        datetime.date(2025, 6, 8),
        TZ_INFO.localize(
            datetime.datetime(2025, 4, 16, 10, 39, 39, 565000), is_dst=True
        ),
        datetime.datetime(2025, 4, 16, 17, 49, 8, 565000),
        "[\n  82,\n  40\n]",
        '{\n  "key1": 71,\n  "key2": 81\n}',
        '{\n  "field1": "f_25",\n  "field2": 25\n}',
        "3-10",
        "18 14:29:08.000000000",
    ),
    (
        -31,
        -15555,
        64403,
        668558045,
        80.87367248535156,
        1413.5031507161045,
        Decimal("9620.13"),
        "str_4635",
        False,
        bytearray(b"\xad\xa9\xdd\xa2"),
        datetime.date(2025, 7, 2),
        TZ_INFO.localize(
            datetime.datetime(2025, 4, 16, 10, 36, 22, 565000), is_dst=True
        ),
        datetime.datetime(2025, 4, 16, 17, 47, 9, 565000),
        "[\n  81,\n  65\n]",
        '{\n  "key1": 67,\n  "key2": 88\n}',
        '{\n  "field1": "f_98",\n  "field2": 69\n}',
        "0-7",
        "19 06:25:08.000000000",
    ),
    (
        114,
        11139,
        75014,
        1135763646,
        14.668656349182129,
        1378.8325065107654,
        Decimal("7411.91"),
        "str_9765",
        False,
        bytearray(b"\xad\xa9\xdd\xa2"),
        datetime.date(2025, 6, 29),
        TZ_INFO.localize(
            datetime.datetime(2025, 4, 16, 10, 48, 27, 565000), is_dst=True
        ),
        datetime.datetime(2025, 4, 16, 17, 50, 8, 565000),
        "[\n  92,\n  27\n]",
        '{\n  "key1": 52,\n  "key2": 65\n}',
        '{\n  "field1": "f_85",\n  "field2": 50\n}',
        "7-4",
        "22 04:52:41.000000000",
    ),
]
EXPECTED_TYPE = StructType(
    [
        StructField("COL_BYTE", LongType(), nullable=True),
        StructField("COL_SHORT", LongType(), nullable=True),
        StructField("COL_INT", LongType(), nullable=True),
        StructField("COL_LONG", LongType(), nullable=True),
        StructField("COL_FLOAT", DoubleType(), nullable=True),
        StructField("COL_DOUBLE", DoubleType(), nullable=True),
        StructField("COL_DECIMAL", DecimalType(10, 2), nullable=True),
        StructField("COL_STRING", StringType(), nullable=True),
        StructField("COL_BOOLEAN", BooleanType(), nullable=True),
        StructField("COL_BINARY", BinaryType(), nullable=True),
        StructField("COL_DATE", DateType(), nullable=True),
        StructField(
            "COL_TIMESTAMP",
            TimestampType(timezone=TimestampTimeZone.LTZ),
            nullable=True,
        ),
        StructField(
            "COL_TIMESTAMP_NTZ",
            TimestampType(timezone=TimestampTimeZone.NTZ),
            nullable=True,
        ),
        StructField("COL_ARRAY", VariantType(), nullable=True),
        StructField("COL_MAP", VariantType(), nullable=True),
        StructField("COL_STRUCT", VariantType(), nullable=True),
        StructField("COL_INTERVAL_YEAR_MONTH", StringType(), nullable=True),
        StructField("COL_INTERVAL_DAY_TIME", StringType(), nullable=True),
    ]
)
DATABRICKS_TEST_EXTERNAL_ACCESS_INTEGRATION = (
    "snowpark_dbapi_databricks_test_integration"
)
