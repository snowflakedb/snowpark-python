#!/usr/bin/env python3
#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

from typing import Iterator

import pytest

try:
    import numpy as np
    import pandas as pd
    from pandas import DataFrame as PandasDF, Series as PandasSeries
    from pandas.testing import assert_frame_equal
except ImportError:
    pytest.skip("pandas is not available", allow_module_level=True)

try:
    import pyarrow as pa
except ImportError:
    pytest.skip("pyarrow is not available", allow_module_level=True)


import datetime
import decimal

import pytest
from unittest import mock

from snowflake.snowpark._internal.utils import TempObjectType
from snowflake.snowpark.session import write_pandas
from snowflake.snowpark.functions import col, div0, round, to_timestamp
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
from tests.utils import IS_IN_STORED_PROC, Utils


def test_to_pandas_new_df_from_range(session):
    # Single column
    snowpark_df = session.range(3, 8)
    pandas_df = snowpark_df.to_pandas()

    assert isinstance(pandas_df, PandasDF)
    assert "ID" in pandas_df
    assert len(pandas_df.columns) == 1
    assert isinstance(pandas_df["ID"], PandasSeries)
    assert all(pandas_df["ID"][i] == i + 3 for i in range(5))

    # Two columns
    snowpark_df = session.range(3, 8).select([col("id"), col("id").alias("other")])
    pandas_df = snowpark_df.to_pandas()

    assert isinstance(pandas_df, PandasDF)
    assert "ID" in pandas_df
    assert "OTHER" in pandas_df
    assert len(pandas_df.columns) == 2
    assert isinstance(pandas_df["ID"], PandasSeries)
    assert all(pandas_df["ID"][i] == i + 3 for i in range(5))
    assert isinstance(pandas_df["OTHER"], PandasSeries)
    assert all(pandas_df["OTHER"][i] == i + 3 for i in range(5))


@pytest.mark.parametrize("to_pandas_api", ["to_pandas", "to_pandas_batches"])
def test_to_pandas_cast_integer(session, to_pandas_api, local_testing_mode):
    snowpark_df = session.create_dataframe(
        [["1", "1" * 20], ["2", "2" * 20]], schema=["a", "b"]
    ).select(
        col("a").cast(DecimalType(2, 0)),
        col("a").cast(DecimalType(4, 0)),
        col("a").cast(DecimalType(6, 0)),
        col("a").cast(DecimalType(18, 0)),
        col("a").cast(IntegerType()),  # equivalent to NUMBER(38,0)
        col("a"),
        col("b").cast(IntegerType()),
    )
    pandas_df = (
        snowpark_df.to_pandas()
        if to_pandas_api == "to_pandas"
        else next(snowpark_df.to_pandas_batches())
    )
    assert str(pandas_df.dtypes.iloc[0]) == "int8"
    assert str(pandas_df.dtypes.iloc[1]) == "int16"
    assert str(pandas_df.dtypes.iloc[2]) == "int32"
    assert str(pandas_df.dtypes.iloc[3]) == "int64"
    assert (
        str(pandas_df.dtypes.iloc[4]) == "int64"
    )  # When limits are not explicitly defined, rely on metadata information from GS.
    assert (
        str(pandas_df.dtypes.iloc[5]) == "object"
    )  # No cast so it's a string. dtype is "object".
    assert (
        str(pandas_df.dtypes.iloc[6]) == "float64"
    )  # A 20-digit number is over int64 max. Convert to float64 in pandas.

    # Make sure timestamp is not accidentally converted to int
    timestamp_snowpark_df = session.create_dataframe([12345], schema=["a"]).select(
        to_timestamp(col("a"))
    )
    timestamp_pandas_df = (
        timestamp_snowpark_df.to_pandas()
        if to_pandas_api == "to_pandas"
        else next(timestamp_snowpark_df.to_pandas_batches())
    )

    if not local_testing_mode:
        # Starting from pyarrow 13, pyarrow no longer coerces non-nanosecond to nanosecond for pandas >=2.0
        # https://arrow.apache.org/release/13.0.0.html and https://github.com/apache/arrow/issues/33321
        pyarrow_major_version = int(pa.__version__.split(".")[0])
        pandas_major_version = int(pd.__version__.split(".")[0])
        expected_dtype = (
            "datetime64[s]"
            if pyarrow_major_version >= 13 and pandas_major_version >= 2
            else "datetime64[ns]"
        )
        assert str(timestamp_pandas_df.dtypes.iloc[0]) == expected_dtype
    else:
        # TODO: mock the non-nanosecond unit pyarrow+pandas behavior in local test
        assert str(timestamp_pandas_df.dtypes.iloc[0]) == "datetime64[ns]"


def test_to_pandas_precision_for_number_38_0(session):
    # Assert that we try to fit into int64 when possible and keep precision

    df = session.create_dataframe(
        [
            [1111111111111111111, 222222222222222222],
            [3333333333333333333, 444444444444444444],
            [5555555555555555555, 666666666666666666],
            [7777777777777777777, 888888888888888888],
            [9223372036854775807, 111111111111111111],
            [2222222222222222222, 333333333333333333],
            [4444444444444444444, 555555555555555555],
            [6666666666666666666, 777777777777777777],
            [-9223372036854775808, 999999999999999999],
        ],
        schema=["A", "B"],
    ).select(
        col("A").cast(DecimalType(38, 0)).alias("A"),
        col("B").cast(DecimalType(18, 0)).alias("B"),
    )

    pdf = df.to_pandas()
    assert pdf["A"][0] == 1111111111111111111
    assert pdf["B"][0] == 222222222222222222
    assert pdf["A"].dtype == "int64"
    assert pdf["B"].dtype == "int64"
    assert pdf["A"].max() == 9223372036854775807
    assert pdf["A"].min() == -9223372036854775808


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="FEAT: div0 and round functions not supported",
)
def test_to_pandas_precision_for_non_zero_scale(session):

    df = session.create_dataframe([[1, 11]], schema=["num1", "num2"]).select(
        col("num1"),
        col("num2"),
        div0(col("num1"), col("num2")).alias("A"),
        div0(col("num1").cast(IntegerType()), col("num2").cast(IntegerType())).alias(
            "B"
        ),
        round(col("B"), 2).alias("C"),
    )

    pdf = df.to_pandas()

    assert pdf["A"].dtype == "float64"
    assert pdf["B"].dtype == "float64"
    assert pdf["C"].dtype == "float64"


@pytest.mark.xfail(
    "config.getoption('local_testing_mode', default=False)",
    reason="SQL query not supported",
    run=False,
)
def test_to_pandas_non_select(session):
    # `with ... select ...` is also a SELECT statement
    isinstance(session.sql("select 1").to_pandas(), PandasDF)
    isinstance(
        session.sql("with mytable as (select 1) select * from mytable").to_pandas(),
        PandasDF,
    )

    def check_fetch_data_exception(query: str):
        df = session.sql(query)
        result = df.to_pandas()
        assert df.columns == result.columns.to_list()
        assert isinstance(result, PandasDF)
        return result

    temp_table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    check_fetch_data_exception("show tables")
    res = check_fetch_data_exception(f"create temporary table {temp_table_name}(a int)")
    expected_res = pd.DataFrame(
        [(f"Table {temp_table_name} successfully created.",)], columns=['"status"']
    )
    assert expected_res.equals(res)
    res = check_fetch_data_exception(f"drop table if exists {temp_table_name}")
    expected_res = pd.DataFrame(
        [(f"{temp_table_name} successfully dropped.",)], columns=['"status"']
    )
    assert expected_res.equals(res)

    # to_pandas should work for the large dataframe
    # batch insertion will run "create" and "insert" first
    df = session.create_dataframe([1] * 2000)
    assert len(df._plan.queries) > 1
    assert df._plan.queries[0].sql.strip().startswith("CREATE")
    assert df._plan.queries[1].sql.strip().startswith("INSERT")
    assert df._plan.queries[2].sql.strip().startswith("SELECT")
    isinstance(df.toPandas(), PandasDF)


def test_to_pandas_for_int_column_with_none_values(session):
    # Assert that we try to fit into int64 when possible and keep precision
    data = [[0], [1], [None]]
    schema = ["A"]
    df = session.create_dataframe(data, schema)

    pdf = df.to_pandas()
    assert pdf["A"][0] == 0
    assert pdf["A"][1] == 1
    assert pd.isna(pdf["A"][2])
    assert pdf["A"].dtype == "float64"


@pytest.mark.skipif(
    IS_IN_STORED_PROC, reason="SNOW-507565: Need localaws for large result"
)
def test_to_pandas_batches(session, local_testing_mode):
    df = session.range(100000).cache_result()
    iterator = df.to_pandas_batches()
    assert isinstance(iterator, Iterator)

    entire_pandas_df = df.to_pandas()
    pandas_df_list = list(df.to_pandas_batches())
    if not local_testing_mode:
        # in live session, large data result will be split into multiple chunks by snowflake
        # local test does not split the data result chunk/is not intended for large data result chunk
        assert len(pandas_df_list) > 1
    assert_frame_equal(pd.concat(pandas_df_list, ignore_index=True), entire_pandas_df)

    for df_batch in df.to_pandas_batches():
        assert_frame_equal(df_batch, entire_pandas_df.iloc[: len(df_batch)])
        break


@pytest.mark.skipif(
    IS_IN_STORED_PROC, reason="SNOW-1362480, backend optimization in different reg env"
)
def test_df_to_pandas_df(session):
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
                    year=2023,
                    month=10,
                    day=30,
                    hour=12,
                    minute=12,
                    second=12,
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
            "AAA": pd.Series([1], dtype=np.int64),
            "BBB": pd.Series([1234567890], dtype=np.int64),
            "CCC": pd.Series([True]),
            "DDD": pd.Series([1.23]),
            "e e": pd.Series(["abc"]),
            "ff ": pd.Series([b"abc"]),
            " gg": pd.Series(
                [
                    datetime.datetime(
                        year=2023,
                        month=10,
                        day=30,
                        hour=12,
                        minute=12,
                        second=12,
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
                StructField("q", MapType()),
            ]
        ),
    )

    pandas_df = pd.DataFrame(
        {
            "A": pd.Series(["[\n  1,\n  2,\n  3,\n  4\n]"], dtype=object),
            "B": pd.Series([b"123"], dtype=object),
            "C": pd.Series([True], dtype=bool),
            "D": pd.Series(
                [1], dtype=np.int64
            ),  # in reg env, there can be backend optimization resulting in np.int8
            "E": pd.Series([datetime.date(year=2023, month=10, day=30)], dtype=object),
            "F": pd.Series([decimal.Decimal(1)], dtype=np.int64),
            "G": pd.Series([1.23], dtype=np.float64),
            "H": pd.Series([1.23], dtype=np.float64),
            "I": pd.Series(
                [100], dtype=np.int64
            ),  # in reg env, there can be backend optimization resulting in np.int8
            "J": pd.Series(
                [100], dtype=np.int64
            ),  # in reg env, there can be backend optimization resulting in np.int8
            "K": pd.Series([None], dtype=object),
            "L": pd.Series(
                [100], dtype=np.int64
            ),  # in reg env, there can be backend optimization resulting in np.int8
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


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="write_pandas is not supported by local testing.",
)
def test_write_pandas_chunk_size(session, monkeypatch):
    table_name = Utils.random_table_name()
    try:
        # create medium-sized df that can be chunked
        df = session.range(101)
        table = df.to_pandas()
        success, num_chunks, num_rows, _ = write_pandas(
            session._conn._conn,
            table,
            table_name,
            auto_create_table=True,
            chunk_size=25,
        )
        # 25 rows per chunk = 5 chunks
        assert success and num_chunks == 5 and num_rows == 101

        # Import the original write_pandas to create a wrapper
        from snowflake.snowpark.session import write_pandas as original_write_pandas

        # Create a wrapper that intercepts calls but lets the real function execute
        def write_pandas_wrapper(*args, **kwargs):
            # Verify that chunk_size=10 was passed
            assert (
                kwargs.get("chunk_size") == 10
            ), f"Expected chunk_size=10, got {kwargs.get('chunk_size')}"
            # Call the real function and return its actual result
            ret = original_write_pandas(*args, **kwargs)
            success, num_chunks, num_rows, _ = ret
            # 10 rows per chunk = 11 chunks
            assert success and num_chunks == 11 and num_rows == 101
            return ret

        with mock.patch(
            "snowflake.snowpark.session.write_pandas", side_effect=write_pandas_wrapper
        ) as mock_write_pandas:
            session.create_dataframe(table, chunk_size=10)
            # Verify that write_pandas was called once
            mock_write_pandas.assert_called_once()
    finally:
        Utils.drop_table(session, table_name)
