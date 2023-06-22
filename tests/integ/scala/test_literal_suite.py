#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#

import datetime
from decimal import Decimal

import pytest

from snowflake.snowpark import Column, Row
from snowflake.snowpark._internal.analyzer.expression import Literal
from snowflake.snowpark.functions import lit
from snowflake.snowpark.types import DecimalType
from tests.utils import Utils

pytestmark = pytest.mark.xfail(
    condition="config.getvalue('local_testing_mode')", raises=NotImplementedError
)


def test_literal_basic_types(session):
    df = (
        session.range(2)
        .with_column("null", lit(None))
        .with_column("str", lit("string"))
        .with_column("char", lit("C"))
        .with_column("bool", lit(True))
        .with_column("bytes", lit(bytes("bytes", "utf8")))
        .with_column("int", lit(12))
        .with_column("float", lit(float(14)))
        .with_column("decimal", lit(Decimal("16")))
    )

    field_str = str(df.schema.fields)

    if session.sql_simplifier_enabled:
        assert (
            field_str == "[StructField('ID', LongType(), nullable=False), "
            "StructField('NULL', StringType(), nullable=True), "
            "StructField('STR', StringType(6), nullable=False), "
            "StructField('CHAR', StringType(1), nullable=False), "
            "StructField('BOOL', BooleanType(), nullable=True), "
            "StructField('BYTES', BinaryType(), nullable=False), "
            "StructField('INT', LongType(), nullable=False), "
            "StructField('FLOAT', DoubleType(), nullable=False), "
            "StructField('DECIMAL', DecimalType(38, 18), nullable=False)]"
        )
    else:
        assert (
            field_str == "[StructField('ID', LongType(), nullable=False), "
            "StructField('NULL', StringType(16777216), nullable=True), "
            "StructField('STR', StringType(6), nullable=False), "
            "StructField('CHAR', StringType(1), nullable=False), "
            "StructField('BOOL', BooleanType(), nullable=True), "
            "StructField('BYTES', BinaryType(), nullable=False), "
            "StructField('INT', LongType(), nullable=False), "
            "StructField('FLOAT', DoubleType(), nullable=False), "
            "StructField('DECIMAL', DecimalType(38, 18), nullable=False)]"
        )

    show_str = df._show_string(10)
    assert (
        show_str
        == """------------------------------------------------------------------------------------------------------------
|"ID"  |"NULL"  |"STR"   |"CHAR"  |"BOOL"  |"BYTES"              |"INT"  |"FLOAT"  |"DECIMAL"              |
------------------------------------------------------------------------------------------------------------
|0     |NULL    |string  |C       |True    |bytearray(b'bytes')  |12     |14.0     |16.000000000000000000  |
|1     |NULL    |string  |C       |True    |bytearray(b'bytes')  |12     |14.0     |16.000000000000000000  |
------------------------------------------------------------------------------------------------------------
"""
    )


def test_literal_timestamp_and_instant(session):
    since_epoch = 1539259994.123  # equivalent to "2018-10-11 12:13:14.123"
    naive_datetime = datetime.datetime.utcfromtimestamp(since_epoch)
    aware_datetime = datetime.datetime.fromtimestamp(since_epoch, datetime.timezone.utc)

    naive_time = datetime.time(12, 13, 14, 123000)
    aware_time = datetime.time(12, 13, 14, 123000, datetime.timezone.utc)

    df = (
        session.range(2)
        .with_column("naive_datetime", lit(naive_datetime))
        .with_column("aware_datetime", lit(aware_datetime))
        .with_column("naive_time", lit(naive_time))
        .with_column("aware_time", lit(aware_time))
    )
    field_str = str(df.schema.fields)
    assert (
        field_str == "[StructField('ID', LongType(), nullable=False), "
        "StructField('NAIVE_DATETIME', TimestampType(), nullable=False), "
        "StructField('AWARE_DATETIME', TimestampType(), nullable=False), "
        "StructField('NAIVE_TIME', TimeType(), nullable=False), "
        "StructField('AWARE_TIME', TimeType(), nullable=False)]"
    )

    show_str = df._show_string(10)
    assert (
        show_str
        == """------------------------------------------------------------------------------------------------------
|"ID"  |"NAIVE_DATETIME"            |"AWARE_DATETIME"            |"NAIVE_TIME"     |"AWARE_TIME"     |
------------------------------------------------------------------------------------------------------
|0     |2018-10-11 12:13:14.123000  |2018-10-11 12:13:14.123000  |12:13:14.123000  |12:13:14.123000  |
|1     |2018-10-11 12:13:14.123000  |2018-10-11 12:13:14.123000  |12:13:14.123000  |12:13:14.123000  |
------------------------------------------------------------------------------------------------------
"""
    )


def test_date(session):
    # dates are always naive
    d = datetime.date(2020, 10, 11)

    df = session.range(2).with_column("date", lit(d))

    field_str = str(df.schema.fields)
    assert (
        field_str == "[StructField('ID', LongType(), nullable=False), "
        "StructField('DATE', DateType(), nullable=False)]"
    )

    show_str = df._show_string(10)
    assert (
        show_str
        == """---------------------
|"ID"  |"DATE"      |
---------------------
|0     |2020-10-11  |
|1     |2020-10-11  |
---------------------
"""
    )


def test_special_literals(session):
    source_literal = lit(123)
    df = (
        session.range(2)
        .with_column("null", lit(None))
        .with_column("literal", lit(source_literal))
    )

    if session.sql_simplifier_enabled:
        assert (
            str(df.schema)
            == "StructType([StructField('ID', LongType(), nullable=False), "
            "StructField('NULL', StringType(), nullable=True), "
            "StructField('LITERAL', LongType(), nullable=False)])"
        )
    else:
        assert (
            str(df.schema)
            == "StructType([StructField('ID', LongType(), nullable=False), "
            "StructField('NULL', StringType(16777216), nullable=True), "
            "StructField('LITERAL', LongType(), nullable=False)])"
        )

    assert (
        df._show_string(10)
        == """
-----------------------------
|"ID"  |"NULL"  |"LITERAL"  |
-----------------------------
|0     |NULL    |123        |
|1     |NULL    |123        |
-----------------------------
""".lstrip()
    )


# This test was originall party of scala-integ tests, but was removed.
def test_special_decimal_literals(session):
    normal_scale = lit(Decimal("0.1"))
    small_scale = Column(Literal(Decimal("0.00001"), DecimalType(5, 5)))

    df = session.range(2).select(normal_scale, small_scale)

    show_str = df._show_string(10)
    assert (
        show_str
        == """-----------------------------------------------------------
|"0.1 ::  NUMBER (38, 18)"  |"0.00001 ::  NUMBER (5, 5)"  |
-----------------------------------------------------------
|0.100000000000000000       |0.00001                      |
|0.100000000000000000       |0.00001                      |
-----------------------------------------------------------
"""
    )


def test_array_object(session):
    df = (
        session.range(1)
        .with_column("list1", lit([1, 2, 3]))
        .with_column("list2", lit([]))
        .with_column("list3", lit([1, "1", 2.5, None]))
        .with_column("tuple1", lit((1, 2, 3)))
        .with_column("tuple2", lit(()))
        .with_column("tuple3", lit((1, "1", 2.5, None)))
        .with_column("dict1", lit({"1": 2.5, "'": "null", '"': None}))
        .with_column("dict2", lit({}))
        .with_column("dict3", lit({"a": [1, "'"], "b": {1: None}}))
    )

    field_str = str(df.schema.fields)
    assert (
        field_str == "[StructField('ID', LongType(), nullable=False), "
        "StructField('LIST1', ArrayType(StringType()), nullable=True), "
        "StructField('LIST2', ArrayType(StringType()), nullable=True), "
        "StructField('LIST3', ArrayType(StringType()), nullable=True), "
        "StructField('TUPLE1', ArrayType(StringType()), nullable=True), "
        "StructField('TUPLE2', ArrayType(StringType()), nullable=True), "
        "StructField('TUPLE3', ArrayType(StringType()), nullable=True), "
        "StructField('DICT1', MapType(StringType(), StringType()), nullable=True), "
        "StructField('DICT2', MapType(StringType(), StringType()), nullable=True), "
        "StructField('DICT3', MapType(StringType(), StringType()), nullable=True)]"
    )
    Utils.check_answer(
        df,
        Row(
            ID=0,
            LIST1="[\n  1,\n  2,\n  3\n]",
            LIST2="[]",
            LIST3='[\n  1,\n  "1",\n  2.5,\n  null\n]',
            TUPLE1="[\n  1,\n  2,\n  3\n]",
            TUPLE2="[]",
            TUPLE3='[\n  1,\n  "1",\n  2.5,\n  null\n]',
            DICT1='{\n  "\\"": null,\n  "\'": "null",\n  "1": 2.5\n}',
            DICT2="{}",
            DICT3='{\n  "a": [\n    1,\n    "\'"\n  ],\n  "b": {\n    "1": null\n  }\n}',
        ),
    )
