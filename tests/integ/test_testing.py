#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import logging
import math

import pytest

from snowflake.snowpark.testing import assert_dataframe_equal, assertDataFrameEqual
from snowflake.snowpark.types import (
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)


@pytest.mark.parametrize(
    "data",
    [
        [[1, "Rice", 1.0], [2, "Saka", 2.0], [3, "White", 3.0]],
        [[1, None, 1.0], [2, "Saka", None], [None, None, None]],
    ],
)
def test_row_basic(session, data):
    schema = StructType(
        [
            StructField("id", IntegerType()),
            StructField("name", StringType()),
            StructField("value", FloatType()),
        ]
    )
    df1 = session.create_dataframe(data, schema)
    df2 = session.create_dataframe(data, schema)
    assert_dataframe_equal(df1, df2)

    # different order
    data[0], data[1] = data[1], data[0]
    df3 = session.create_dataframe(data, schema)
    assert_dataframe_equal(df1, df3)

    # different value
    data[0][0] = 10000
    df4 = session.create_dataframe(data, schema)
    with pytest.raises(AssertionError, match="Different row:"):
        assert_dataframe_equal(df3, df4)

    # null
    data[0][0] = None
    df5 = session.create_dataframe(data, schema)
    with pytest.raises(AssertionError, match="Different row:"):
        assert_dataframe_equal(df3, df5)

    # more rows
    df5 = session.create_dataframe(data + [[4, "Rowe", 4.0]], schema)
    with pytest.raises(AssertionError, match="Different number of rows"):
        assert_dataframe_equal(df3, df5)


def test_row_data_float(session):
    schema = StructType(
        [StructField("id", IntegerType()), StructField("value", DoubleType())]
    )
    data1 = [(1, 1.0), (2, 2.0)]
    data2 = [(1, 1.0), (2, 2.000001)]
    df1 = session.create_dataframe(data1, schema)
    df2 = session.create_dataframe(data2, schema)
    assert_dataframe_equal(df1, df2)

    data3 = [(1, 1.0), (2, 2.0001)]
    df3 = session.create_dataframe(data3, schema)
    with pytest.raises(AssertionError, match="Different row:"):
        assert_dataframe_equal(df1, df3)
    assert_dataframe_equal(df1, df3, atol=1e3)


def test_row_data_nan(session):
    schema = StructType(
        [StructField("id", IntegerType()), StructField("value", DoubleType())]
    )
    data1 = [(1, 1.0), (2, math.nan)]
    df1 = session.create_dataframe(data1, schema)
    df2 = session.create_dataframe(data1, schema)
    assert_dataframe_equal(df1, df2)

    data2 = [(1, math.nan), (2, math.nan)]
    df3 = session.create_dataframe(data2, schema)
    with pytest.raises(AssertionError, match="Different row:"):
        assert_dataframe_equal(df1, df3)

    data3 = [(1, 1.0), (2, None)]
    df4 = session.create_dataframe(data3, schema)
    with pytest.raises(AssertionError, match="Different row:"):
        assert_dataframe_equal(df1, df4)


def test_schema_basic(session):
    schema1 = StructType(
        [
            StructField("id", IntegerType()),
            StructField("name", StringType()),
            StructField("value", FloatType()),
        ]
    )
    df1 = session.create_dataframe([], schema=schema1)
    df2 = session.create_dataframe([], schema=schema1)
    assert_dataframe_equal(df1, df2)

    # different number of columns
    schema2 = StructType(
        [
            StructField("id", IntegerType()),
            StructField("'key'", StringType()),
        ]
    )
    df3 = session.create_dataframe([], schema=schema2)
    with pytest.raises(AssertionError, match="Different number of columns"):
        assert_dataframe_equal(df1, df3)


def test_schema_column_name(session):
    schema1 = StructType(
        [
            StructField("id", IntegerType()),
            StructField("name", StringType()),
            StructField("value", FloatType()),
        ]
    )
    df1 = session.create_dataframe([], schema=schema1)
    schema2 = StructType(
        [
            StructField("id", IntegerType()),
            StructField("key", StringType()),
            StructField("value", FloatType()),
        ]
    )
    df2 = session.create_dataframe([], schema=schema2)
    with pytest.raises(AssertionError, match="Different schema:"):
        assert_dataframe_equal(df1, df2)
    schema3 = StructType(
        [
            StructField("id", IntegerType()),
            StructField('"name"', StringType()),
            StructField("value", FloatType()),
        ]
    )
    df3 = session.create_dataframe([], schema=schema3)
    with pytest.raises(AssertionError, match="Different schema:"):
        assert_dataframe_equal(df1, df3)
    schema4 = StructType(
        [
            StructField("id", IntegerType()),
            StructField("NAME", StringType()),
            StructField("value", FloatType()),
        ]
    )
    df4 = session.create_dataframe([], schema=schema4)
    assert_dataframe_equal(df1, df4)
    schema5 = StructType(
        [
            StructField("id", IntegerType()),
            StructField('"NAME"', StringType()),
            StructField("value", FloatType()),
        ]
    )
    df5 = session.create_dataframe([], schema=schema5)
    assert_dataframe_equal(df1, df5)


def test_schema_datatype(session):
    schema1 = StructType(
        [
            StructField("id", IntegerType()),
            StructField("name", StringType()),
            StructField("value", FloatType()),
        ]
    )
    df1 = session.create_dataframe([], schema=schema1)
    schema2 = StructType(
        [
            StructField("id", FloatType()),
            StructField("name", StringType()),
            StructField("value", FloatType()),
        ]
    )
    df2 = session.create_dataframe([], schema=schema2)
    with pytest.raises(AssertionError, match="Different schema:"):
        assert_dataframe_equal(df1, df2)
    schema3 = StructType(
        [
            StructField("id", LongType()),
            StructField("name", StringType()),
            StructField("value", DoubleType()),
        ]
    )
    df3 = session.create_dataframe([], schema=schema3)
    assert_dataframe_equal(df1, df3)
    schema4 = StructType(
        [
            StructField("id", IntegerType()),
            StructField("name", StringType(2)),
            StructField("value", FloatType()),
        ]
    )
    df4 = session.create_dataframe([], schema=schema4)
    with pytest.raises(AssertionError, match="Different schema:"):
        assert_dataframe_equal(df1, df4)


def test_schema_nullable(session):
    schema1 = StructType(
        [
            StructField("id", IntegerType()),
            StructField("name", StringType()),
            StructField("value", FloatType()),
        ]
    )
    df1 = session.create_dataframe([], schema=schema1)
    schema2 = StructType(
        [
            StructField("id", IntegerType(), nullable=False),
            StructField("name", StringType()),
            StructField("value", FloatType()),
        ]
    )
    df2 = session.create_dataframe([], schema=schema2)
    with pytest.raises(AssertionError, match="Different schema:"):
        assert_dataframe_equal(df1, df2)


def test_input_not_snowpark_df(session):
    try:
        import pandas
    except ImportError:
        pytest.skip("No pandas is available")

    data = [1]
    df = session.create_dataframe(data)
    pandas_df = pandas.DataFrame(data)
    with pytest.raises(TypeError, match="must be a Snowpark DataFrame"):
        assert_dataframe_equal(df, data)
    with pytest.raises(TypeError, match="must be a Snowpark DataFrame"):
        assert_dataframe_equal(pandas_df, df)


def test_alias():
    assert assert_dataframe_equal == assertDataFrameEqual


def test_experimental(session, caplog):
    df = session.create_dataframe([1])
    caplog.clear()
    with caplog.at_level(logging.WARNING):
        assert_dataframe_equal(df, df)
    assert "is experimental since" in caplog.text
