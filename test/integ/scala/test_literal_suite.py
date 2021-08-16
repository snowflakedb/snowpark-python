#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All right reserved.
#

import datetime
from decimal import Decimal

from snowflake.snowpark.column import Column
from snowflake.snowpark.functions import lit
from snowflake.snowpark.internal.sp_expressions import Literal
from snowflake.snowpark.types.sp_data_types import DecimalType


def test_literal_basic_types(session_cnx):
    with session_cnx() as session:
        df = (
            session.range(2)
            .withColumn("null", lit(None))
            .withColumn("str", lit("string"))
            .withColumn("char", lit("C"))
            .withColumn("bool", lit(True))
            .withColumn("bytes", lit(bytes("bytes", "utf8")))
            .withColumn("int", lit(12))
            .withColumn("float", lit(float(14)))
            .withColumn("decimal", lit(Decimal("16")))
        )

        field_str = str(df.schema.fields)

        assert (
            field_str == "[StructField(ID, Long, Nullable=False), "
            "StructField(NULL, String, Nullable=True), "
            "StructField(STR, String, Nullable=False), "
            "StructField(CHAR, String, Nullable=False), "
            "StructField(BOOL, Boolean, Nullable=True), "
            "StructField(BYTES, Binary, Nullable=False), "
            "StructField(INT, Long, Nullable=False), "
            "StructField(FLOAT, Double, Nullable=False), "
            "StructField(DECIMAL, Decimal(38,18), Nullable=False)]"
        )

        show_str = df._DataFrame__show_string(10)
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


def test_literal_timestamp_and_instant(session_cnx):
    with session_cnx() as session:
        since_epoch = 1539259994.123  # equivalent to "2018-10-11 12:13:14.123"
        naive_datetime = datetime.datetime.utcfromtimestamp(since_epoch)
        aware_datetime = datetime.datetime.fromtimestamp(
            since_epoch, datetime.timezone.utc
        )

        naive_time = datetime.time(12, 13, 14, 123000)
        aware_time = datetime.time(12, 13, 14, 123000, datetime.timezone.utc)

        df = (
            session.range(2)
            .withColumn("naive_datetime", lit(naive_datetime))
            .withColumn("aware_datetime", lit(aware_datetime))
            .withColumn("naive_time", lit(naive_time))
            .withColumn("aware_time", lit(aware_time))
        )
        field_str = str(df.schema.fields)
        assert (
            field_str == "[StructField(ID, Long, Nullable=False), "
            "StructField(NAIVE_DATETIME, Timestamp, Nullable=False), "
            "StructField(AWARE_DATETIME, Timestamp, Nullable=False), "
            "StructField(NAIVE_TIME, Time, Nullable=False), "
            "StructField(AWARE_TIME, Time, Nullable=False)]"
        )

        show_str = df._DataFrame__show_string(10)
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


def test_date(session_cnx):
    with session_cnx() as session:
        # dates are always naive
        d = datetime.date(2020, 10, 11)

        df = session.range(2).withColumn("date", lit(d))

        field_str = str(df.schema.fields)
        assert (
            field_str == "[StructField(ID, Long, Nullable=False), "
            "StructField(DATE, Date, Nullable=False)]"
        )

        show_str = df._DataFrame__show_string(10)
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


def test_special_bigdecimal_literals(session_cnx):
    with session_cnx() as session:
        normal_scale = lit(Decimal("0.1"))
        small_scale = Column(Literal(Decimal("0.00001"), DecimalType(5, 5)))

        df = session.range(2).select(normal_scale, small_scale)

        show_str = df._DataFrame__show_string(10)
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
