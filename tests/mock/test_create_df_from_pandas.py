#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#
import datetime
import decimal
import json
import math

import pandas as pd
import pytest
import pytz

from snowflake.snowpark import Row, Session, Table
from snowflake.snowpark.mock.connection import MockServerConnection
from snowflake.snowpark.types import BooleanType, DoubleType, LongType, StringType

session = Session(MockServerConnection())


@pytest.mark.localtest
def test_create_from_pandas_basic_pandas_types():
    now_time = datetime.datetime(
        year=2023, month=10, day=25, hour=13, minute=46, second=12, microsecond=123
    )
    delta_time = datetime.timedelta(days=1)
    pandas_df = pd.DataFrame(
        data=[
            ("Name1", 1.2, 1234567890, True, now_time, delta_time),
            ("nAme_2", 20, 1, False, now_time - delta_time, delta_time),
        ],
        columns=[
            "sTr",
            "dOublE",
            "LoNg",
            "booL",
            "timestamp",
            "TIMEDELTA",  # note that in the current snowpark, column name with all upper case is not double quoted
        ],
    )
    sp_df = session.create_dataframe(data=pandas_df)
    assert (
        sp_df.schema[0].name == '"sTr"'
        and isinstance(sp_df.schema[0].datatype, StringType)
        and sp_df.schema[0].nullable
    )
    assert (
        sp_df.schema[1].name == '"dOublE"'
        and isinstance(sp_df.schema[1].datatype, DoubleType)
        and sp_df.schema[1].nullable
    )
    assert (
        sp_df.schema[2].name == '"LoNg"'
        and isinstance(sp_df.schema[2].datatype, LongType)
        and sp_df.schema[2].nullable
    )
    assert (
        sp_df.schema[3].name == '"booL"'
        and isinstance(sp_df.schema[3].datatype, BooleanType)
        and sp_df.schema[3].nullable
    )
    assert (
        sp_df.schema[4].name == '"timestamp"'
        and isinstance(sp_df.schema[4].datatype, LongType)
        and sp_df.schema[4].nullable
    )
    assert (
        sp_df.schema[5].name == "TIMEDELTA"
        and isinstance(sp_df.schema[5].datatype, LongType)
        and sp_df.schema[5].nullable
    )
    assert isinstance(sp_df, Table)
    assert (
        str(sp_df.schema)
        == """\
StructType([\
StructField('"sTr"', StringType(), nullable=True), \
StructField('"dOublE"', DoubleType(), nullable=True), \
StructField('"LoNg"', LongType(), nullable=True), \
StructField('"booL"', BooleanType(), nullable=True), \
StructField('"timestamp"', LongType(), nullable=True), \
StructField('TIMEDELTA', LongType(), nullable=True)\
])\
"""
    )
    assert sp_df.select('"sTr"').collect() == [Row("Name1"), Row("nAme_2")]
    assert sp_df.select('"dOublE"').collect() == [Row(1.2), Row(20)]
    assert sp_df.select('"LoNg"').collect() == [Row(1234567890), Row(1)]
    assert sp_df.select('"booL"').collect() == [Row(True), Row(False)]
    assert sp_df.select('"timestamp"').collect() == [
        Row(1698241572000123),
        Row(1698155172000123),
    ]
    assert sp_df.select("TIMEDELTA").collect() == [
        Row(86400000000000),
        Row(86400000000000),
    ]

    pandas_df = pd.DataFrame(
        data=[
            float("inf"),
            float("-inf"),
        ],
        columns=["float"],
    )
    sp_df = session.create_dataframe(data=pandas_df)
    assert (
        sp_df.schema[0].name == '"float"'
        and isinstance(sp_df.schema[0].datatype, DoubleType)
        and sp_df.schema[0].nullable
    )

    assert sp_df.select('"float"').collect() == [
        Row(float("inf")),
        Row(float("-inf")),
    ]


@pytest.mark.localtest
def test_create_from_pandas_basic_python_types():
    date_data = datetime.date(year=2023, month=10, day=26)
    time_data = datetime.time(hour=12, minute=12, second=12)
    byte_data = b"bytedata"
    dict_data = {"a": 123}
    array_data = [1, 2, 3, 4]
    decimal_data = decimal.Decimal("1.23")
    pandas_df = pd.DataFrame(
        {
            "A": pd.Series([date_data]),
            "B": pd.Series([time_data]),
            "C": pd.Series([byte_data]),
            "D": pd.Series([dict_data]),
            "E": pd.Series([array_data]),
            "F": pd.Series([decimal_data]),
        }
    )
    sp_df = session.create_dataframe(data=pandas_df)
    assert (
        str(sp_df.schema)
        == """\
StructType([StructField('A', DateType(), nullable=True), StructField('B', TimeType(), nullable=True), StructField('C', BinaryType(), nullable=True), StructField('D', VariantType(), nullable=True), StructField('E', VariantType(), nullable=True), StructField('F', DecimalType(3, 2), nullable=True)])\
"""
    )
    assert sp_df.select("*").collect() == [
        Row(
            date_data,
            time_data,
            bytearray(byte_data),
            json.dumps(dict_data, indent=2),
            json.dumps(array_data, indent=2),
            decimal_data,
        )
    ]


@pytest.mark.localtest
def test_create_from_pandas_datetime_types():
    now_time = datetime.datetime(
        year=2023,
        month=10,
        day=25,
        hour=13,
        minute=46,
        second=12,
        microsecond=123,
        tzinfo=pytz.UTC,
    )
    now_time_without_tz = datetime.datetime(
        year=2023, month=10, day=25, hour=13, minute=46, second=12, microsecond=123
    )
    delta_time = datetime.timedelta(days=1)
    pandas_df = pd.DataFrame(
        {
            "A": pd.Series([now_time], dtype="datetime64[ns]"),
            "B": pd.Series([delta_time], dtype="timedelta64[ns]"),
            "C": pd.Series([now_time], dtype=pd.DatetimeTZDtype(tz=pytz.UTC)),
        }
    )
    sp_df = session.create_dataframe(data=pandas_df)
    assert sp_df.select("A").collect() == [Row(1698241572000123)]
    assert sp_df.select("B").collect() == [Row(86400000000000)]
    assert sp_df.select("C").collect() == [Row(now_time_without_tz)]

    pandas_df = pd.DataFrame(
        {
            "A": pd.Series(
                [
                    datetime.datetime(
                        1997,
                        6,
                        3,
                        14,
                        21,
                        32,
                        00,
                        tzinfo=datetime.timezone(datetime.timedelta(hours=+10)),
                    )
                ]
            )
        }
    )
    sp_df = session.create_dataframe(data=pandas_df)
    assert (
        str(sp_df.schema)
        == "StructType([StructField('A', TimestampType(), nullable=True)])"
    )
    assert sp_df.select("A").collect() == [
        Row(datetime.datetime(1997, 6, 3, 4, 21, 32, 00))
    ]


@pytest.mark.localtest
def test_create_from_pandas_extension_types():
    """

    notes:
        pd.SparseDtype is not supported in the live mode due to pyarrow
    """
    pandas_df = pd.DataFrame(
        {
            "A": pd.Series(["a", "b", "c", "a"], dtype=pd.CategoricalDtype()),
        }
    )
    sp_df = session.create_dataframe(data=pandas_df)
    assert sp_df.select("A").collect() == [Row("a"), Row("b"), Row("c"), Row("a")]

    pandas_df = pd.DataFrame(
        {
            "A": pd.Series([1, 2, 3], dtype=pd.Int8Dtype()),
            "B": pd.Series([1, 2, 3], dtype=pd.Int16Dtype()),
            "C": pd.Series([1, 2, 3], dtype=pd.Int32Dtype()),
            "D": pd.Series([1, 2, 3], dtype=pd.Int64Dtype()),
            "E": pd.Series([1, 2, 3], dtype=pd.UInt8Dtype()),
            "F": pd.Series([1, 2, 3], dtype=pd.UInt16Dtype()),
            "G": pd.Series([1, 2, 3], dtype=pd.UInt32Dtype()),
            "H": pd.Series([1, 2, 3], dtype=pd.UInt64Dtype()),
        }
    )
    sp_df = session.create_dataframe(data=pandas_df)
    assert (
        sp_df.select("A").collect()
        == sp_df.select("B").collect()
        == sp_df.select("C").collect()
        == sp_df.select("D").collect()
        == sp_df.select("E").collect()
        == sp_df.select("F").collect()
        == sp_df.select("G").collect()
        == sp_df.select("H").collect()
        == [Row(1), Row(2), Row(3)]
    )

    pandas_df = pd.DataFrame(
        {
            "A": pd.Series([1.1, 2.2, 3.3], dtype=pd.Float32Dtype()),
        }
    )
    sp_df = session.create_dataframe(data=pandas_df)
    assert sp_df.select("A").collect() == [Row(1.1), Row(2.2), Row(3.3)]

    pandas_df = pd.DataFrame(
        {
            "A": pd.Series([1.1, 2.2, 3.3], dtype=pd.Float64Dtype()),
        }
    )
    sp_df = session.create_dataframe(data=pandas_df)
    assert sp_df.select("A").collect() == [Row(1.1), Row(2.2), Row(3.3)]

    pandas_df = pd.DataFrame(
        {
            "A": pd.Series(["a", "b", "c"], dtype=pd.StringDtype()),
        }
    )
    sp_df = session.create_dataframe(data=pandas_df)
    assert sp_df.select("A").collect() == [Row("a"), Row("b"), Row("c")]

    pandas_df = pd.DataFrame(
        {
            "A": pd.Series([True, False, True], dtype=pd.BooleanDtype()),
        }
    )
    sp_df = session.create_dataframe(data=pandas_df)
    assert sp_df.select("A").collect() == [Row(True), Row(False), Row(True)]

    pandas_df = pd.DataFrame(
        {
            "A": pd.Series([pd.Period("2022-01", freq="M")], dtype=pd.PeriodDtype()),
        }
    )
    sp_df = session.create_dataframe(data=pandas_df)
    assert sp_df.select("A").collect() == [Row(624)]

    pandas_df = pd.DataFrame(
        {
            "A": pd.Series([pd.Interval(left=0, right=5)], dtype=pd.IntervalDtype()),
            "B": pd.Series(
                [
                    pd.Interval(
                        pd.Timestamp("2017-01-01 00:00:00"),
                        pd.Timestamp("2018-01-01 00:00:00"),
                    )
                ],
                dtype=pd.IntervalDtype(),
            ),
        }
    )
    sp_df = session.create_dataframe(data=pandas_df)
    ret = sp_df.select("*").collect()
    assert (
        str(sp_df.schema)
        == """\
StructType([StructField('A', VariantType(), nullable=True), StructField('B', VariantType(), nullable=True)])\
"""
    )
    assert (
        str(ret)
        == """\
[Row(A='{\\n  "left": 0,\\n  "right": 5\\n}', B='{\\n  "left": 1483228800000000,\\n  "right": 1514764800000000\\n}')]\
"""
    )
    assert ret == [
        Row(
            '{\n  "left": 0,\n  "right": 5\n}',
            '{\n  "left": 1483228800000000,\n  "right": 1514764800000000\n}',
        )
    ]


@pytest.mark.localtest
def test_na_and_null_data():
    pandas_df = pd.DataFrame(
        data={
            "A": pd.Series([1, None, 2, math.nan]),
        }
    )
    sp_df = session.create_dataframe(data=pandas_df)
    assert sp_df.select("A").collect() == [Row(1.0), Row(None), Row(2.0), Row(None)]

    pandas_df = pd.DataFrame(
        data={
            "A": pd.Series(["abc", None, "a", ""]),
        }
    )
    sp_df = session.create_dataframe(data=pandas_df)
    assert sp_df.select("A").collect() == [Row("abc"), Row(None), Row("a"), Row("")]
