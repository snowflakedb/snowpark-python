#!/usr/bin/env python3
#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#

from typing import Iterator

import pytest

try:
    import pandas as pd
    from pandas import DataFrame as PandasDF, Series as PandasSeries
    from pandas.testing import assert_frame_equal
except ImportError:
    pytest.skip("Pandas is not available", allow_module_level=True)

try:
    import pyarrow as pa
except ImportError:
    pytest.skip("pyarrow is not available", allow_module_level=True)


from snowflake.snowpark._internal.utils import TempObjectType
from snowflake.snowpark.exceptions import SnowparkFetchDataException
from snowflake.snowpark.functions import col, to_timestamp
from snowflake.snowpark.types import DecimalType, IntegerType
from tests.utils import IS_IN_STORED_PROC, Utils


@pytest.mark.localtest
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


@pytest.mark.localtest
@pytest.mark.parametrize("to_pandas_api", ["to_pandas", "to_pandas_batches"])
def test_to_pandas_cast_integer(session, to_pandas_api, local_testing_mode):
    snowpark_df = session.create_dataframe(
        [["1", "1" * 20], ["2", "2" * 20]], schema=["a", "b"]
    ).select(
        col("a").cast(DecimalType(2, 0)),
        col("a").cast(DecimalType(4, 0)),
        col("a").cast(DecimalType(6, 0)),
        col("a").cast(DecimalType(18, 0)),
        col("a").cast(IntegerType()),
        col("a"),
        col("b").cast(IntegerType()),
    )
    pandas_df = (
        snowpark_df.to_pandas()
        if to_pandas_api == "to_pandas"
        else next(snowpark_df.to_pandas_batches())
    )
    assert str(pandas_df.dtypes[0]) == "int8"
    assert str(pandas_df.dtypes[1]) == "int16"
    assert str(pandas_df.dtypes[2]) == "int32"
    assert str(pandas_df.dtypes[3]) == "int64"
    assert (
        str(pandas_df.dtypes[4]) == "int8"
    )  # When static type can possibly be greater than int64 max, use the actual value to infer the int type.
    assert (
        str(pandas_df.dtypes[5]) == "object"
    )  # No cast so it's a string. dtype is "object".
    assert (
        str(pandas_df.dtypes[6]) == "float64"
    )  # A 20-digit number is over int64 max. Convert to float64 in Pandas.

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
        assert str(timestamp_pandas_df.dtypes[0]) == expected_dtype
    else:
        # TODO: mock the non-nanosecond unit pyarrow+pandas behavior in local test
        assert str(timestamp_pandas_df.dtypes[0]) == "datetime64[ns]"


def test_to_pandas_precision_for_number_38_0(session):
    # Assert that we try to fit into int64 when possible and keep precision
    df = session.sql(
        """
    SELECT
        CAST(COLUMN1 as NUMBER(38,0)) AS A,
        CAST(COLUMN2 as NUMBER(18,0)) AS B
    FROM VALUES
        (1111111111111111111, 222222222222222222),
        (3333333333333333333, 444444444444444444),
        (5555555555555555555, 666666666666666666),
        (7777777777777777777, 888888888888888888),
        (9223372036854775807, 111111111111111111),
        (2222222222222222222, 333333333333333333),
        (4444444444444444444, 555555555555555555),
        (6666666666666666666, 777777777777777777),
        (-9223372036854775808, 999999999999999999)
        """
    )

    pdf = df.to_pandas()
    assert pdf["A"][0] == 1111111111111111111
    assert pdf["B"][0] == 222222222222222222
    assert pdf["A"].dtype == "int64"
    assert pdf["B"].dtype == "int64"
    assert pdf["A"].max() == 9223372036854775807
    assert pdf["A"].min() == -9223372036854775808


def test_to_pandas_non_select(session):
    # `with ... select ...` is also a SELECT statement
    isinstance(session.sql("select 1").to_pandas(), PandasDF)
    isinstance(
        session.sql("with mytable as (select 1) select * from mytable").to_pandas(),
        PandasDF,
    )

    # non SELECT statements will fail
    def check_fetch_data_exception(query: str) -> None:
        with pytest.raises(SnowparkFetchDataException) as ex_info:
            session.sql(query).to_pandas()
        assert "the input query can only be a SELECT statement" in str(ex_info.value)

    temp_table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    check_fetch_data_exception("show tables")
    check_fetch_data_exception(f"create temporary table {temp_table_name}(a int)")
    check_fetch_data_exception(f"drop table if exists {temp_table_name}")

    # to_pandas should work for the large dataframe
    # batch insertion will run "create" and "insert" first
    df = session.create_dataframe([1] * 2000)
    assert len(df._plan.queries) > 1
    assert df._plan.queries[0].sql.strip().startswith("CREATE")
    assert df._plan.queries[1].sql.strip().startswith("INSERT")
    assert df._plan.queries[2].sql.strip().startswith("SELECT")
    isinstance(df.toPandas(), PandasDF)


@pytest.mark.skipif(
    IS_IN_STORED_PROC, reason="SNOW-507565: Need localaws for large result"
)
@pytest.mark.localtest
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
