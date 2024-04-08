#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#
import datetime
import decimal

import pytest

from snowflake.snowpark import Session
from snowflake.snowpark.mock._connection import MockServerConnection
from snowflake.snowpark.types import (
    ArrayType,
    BinaryType,
    BooleanType,
    ByteType,
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    MapType,
    NullType,
    ShortType,
    StringType,
    StructField,
    StructType,
    TimestampType,
    TimeType,
    VariantType,
)

try:
    import numpy as np
    import pandas as pd
    from pandas.testing import assert_frame_equal
except ImportError:
    pytest.skip("pandas is not installed, skipping the tests", allow_module_level=True)


session = Session(MockServerConnection())


@pytest.mark.localtest
def test_df_to_pandas_df():
    df = session.create_dataframe(
        [
            [
                1,
                1234567890,
                True,
                1.23,
                "abc",
                b"abc",
                datetime.datetime(
                    year=2023, month=10, day=30, hour=12, minute=12, second=12
                ),
            ]
        ],
        schema=[
            "aaa",
            "BBB",
            "cCc",
            "DdD",
            "e e",
            "ff ",
            " gg",
        ],
    )

    to_compare_df = pd.DataFrame(
        {
            "AAA": pd.Series(
                [1], dtype=np.int8
            ),  # int8 is the snowpark behavior, by default pandas use int64
            "BBB": pd.Series(
                [1234567890], dtype=np.int32
            ),  # int32 is the snowpark behavior, by default pandas use int64
            "CCC": pd.Series([True]),
            "DDD": pd.Series([1.23]),
            "e e": pd.Series(["abc"]),
            "ff ": pd.Series([b"abc"]),
            " gg": pd.Series(
                [
                    datetime.datetime(
                        year=2023, month=10, day=30, hour=12, minute=12, second=12
                    )
                ]
            ),
        }
    )

    # assert_frame_equal also checks dtype
    assert_frame_equal(df.to_pandas(), to_compare_df)
    assert_frame_equal(list(df.to_pandas_batches())[0], to_compare_df)

    # check snowflake types explicitly
    df = session.create_dataframe(
        data=[
            [
                [1, 2, 3, 4],
                b"123",
                True,
                1,
                datetime.date(year=2023, month=10, day=30),
                decimal.Decimal(1),
                1.23,
                1.23,
                100,
                100,
                None,
                100,
                "abc",
                datetime.datetime(2023, 10, 30, 12, 12, 12),
                datetime.time(12, 12, 12),
                {"a": "b"},
                {"a": "b"},
            ],
        ],
        schema=StructType(
            [
                StructField("a", ArrayType()),
                StructField("b", BinaryType()),
                StructField("c", BooleanType()),
                StructField("d", ByteType()),
                StructField("e", DateType()),
                StructField("f", DecimalType()),
                StructField("g", DoubleType()),
                StructField("h", FloatType()),
                StructField("i", IntegerType()),
                StructField("j", LongType()),
                StructField("k", NullType()),
                StructField("l", ShortType()),
                StructField("m", StringType()),
                StructField("n", TimestampType()),
                StructField("o", TimeType()),
                StructField("p", VariantType()),
                StructField("q", MapType(StringType(), StringType())),
            ]
        ),
    )

    pandas_df = pd.DataFrame(
        {
            "A": pd.Series(["[\n  1,\n  2,\n  3,\n  4\n]"], dtype=object),
            "B": pd.Series([b"123"], dtype=object),
            "C": pd.Series([True], dtype=bool),
            "D": pd.Series([1], dtype=np.int8),
            "E": pd.Series([datetime.date(year=2023, month=10, day=30)], dtype=object),
            "F": pd.Series([decimal.Decimal(1)], dtype=np.int8),
            "G": pd.Series([1.23], dtype=np.float64),
            "H": pd.Series([1.23], dtype=np.float64),
            "I": pd.Series([100], dtype=np.int8),
            "J": pd.Series([100], dtype=np.int8),
            "K": pd.Series([None], dtype=object),
            "L": pd.Series([100], dtype=np.int8),
            "M": pd.Series(["abc"], dtype=object),
            "N": pd.Series(
                [datetime.datetime(2023, 10, 30, 12, 12, 12)], dtype="datetime64[ns]"
            ),
            "O": pd.Series([datetime.time(12, 12, 12)], dtype=object),
            "P": pd.Series(['{\n  "a": "b"\n}'], dtype=object),
            "Q": pd.Series(['{\n  "a": "b"\n}'], dtype=object),
        }
    )
    assert_frame_equal(df.to_pandas(), pandas_df)
