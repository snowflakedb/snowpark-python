#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd

# This test file contains tests that execute a single underlying snowpark/snowflake pivot query.
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from snowflake.snowpark.exceptions import SnowparkSQLException
from tests.integ.modin.pivot.pivot_utils import (
    pivot_table_test_helper,
    pivot_table_test_helper_expects_exception,
)
from tests.integ.modin.sql_counter import sql_count_checker
from tests.integ.modin.utils import create_test_dfs, eval_snowpark_pandas_result


@pytest.mark.skip(
    "SNOW-959913: Support no index configuration with columns and margins configuration"
)
@sql_count_checker(query_count=1)
def test_pivot_table_no_index_single_column_single_value(df_data):
    pivot_table_test_helper(
        df_data,
        {
            "index": None,
            "columns": "C",
            "values": "D",
        },
    )


@pytest.mark.parametrize(
    "aggfunc",
    [
        "mean",
        "sum",
        "min",
        "max",
        "count",
    ],
)
@sql_count_checker(query_count=1)
def test_pivot_table_single_index_single_column_single_value(df_data, aggfunc):
    pivot_table_test_helper(
        df_data,
        {
            "index": "A",
            "columns": "C",
            "values": "D",
            "aggfunc": aggfunc,
        },
    )


@pytest.mark.parametrize(
    "aggfunc",
    [
        "count",
        "sum",
        "min",
        "max",
        "mean",
    ],
)
@sql_count_checker(query_count=1)
def test_pivot_table_multi_index_single_column_single_value(df_data, aggfunc):
    pivot_table_test_helper(
        df_data,
        {"index": ["A", "B"], "columns": "C", "values": "D", "aggfunc": aggfunc},
    )


@sql_count_checker(query_count=1)
def test_pivot_table_empty_table_with_groupby_columns():
    # Cannot use pivot_table_test_helper since that checks the inferred types
    # on the resulting DataFrames' columns (which are empty), and the inferred type
    # on our DataFrame's columns is empty, while pandas has type floating.
    import pandas as native_pd

    native_df = native_pd.DataFrame({"A": [], "B": [], "C": [], "D": []})
    snow_df = pd.DataFrame(native_df)
    pivot_kwargs = {
        "index": ["A", "B"],
        "columns": "C",
        "values": "D",
        "aggfunc": "count",
    }

    snow_result = snow_df.pivot_table(**pivot_kwargs).to_pandas()
    native_result = native_df.pivot_table(**pivot_kwargs)

    assert native_result.empty == snow_result.empty and (native_result.empty is True)
    assert list(native_result.columns) == list(snow_result.columns)
    assert list(native_result.index) == list(snow_result.index)


@sql_count_checker(query_count=1)
def test_pivot_table_single_index_no_column_single_value(df_data):
    pivot_table_test_helper(
        df_data,
        {
            "index": "A",
            "columns": None,
            "values": "D",
        },
    )


@sql_count_checker(query_count=1)
def test_pivot_table_multi_index_no_column_single_value(df_data):
    pivot_table_test_helper(
        df_data,
        {
            "index": ["A", "B"],
            "columns": None,
            "values": "D",
        },
    )


@sql_count_checker(query_count=0)
def test_pivot_table_no_index_no_column_single_value(df_data):
    pivot_table_test_helper_expects_exception(
        df_data,
        {
            "index": None,
            "columns": None,
            "values": "D",
        },
        expect_exception_match="pivot_table with no index configuration is currently not supported",
        expect_exception_type=NotImplementedError,
        assert_exception_equal=False,
    )


@pytest.mark.xfail(
    strict=True,
    reason="SNOW-1201994: index contains ints coerced to string",
    # df_data_with_duplicates is wrapped in a call to `np.array` that coerces the
    # provided integer literals to str.
    # pandas 2.1 no longer allows the calculation of mean() on string columns, and now fails.
    # Though the data is now fixed to contain the appropriate mix of strings and ints, the test
    # now fails because the column names of the Snowpark pandas pivot_table result are coerced
    # to string instead of staying as int.
    # https://github.com/pandas-dev/pandas/issues/36703
    # https://github.com/pandas-dev/pandas/issues/44008
)
@sql_count_checker(query_count=1, join_count=1)
def test_pivot_table_with_duplicate_values(
    df_data_with_duplicates,
):
    pivot_table_test_helper(
        df_data_with_duplicates,
        {
            "index": "C",
            "columns": "E",
            "values": "D",
        },
        # Duplicates aren't handled currently for coercing and not needed for this particular test.
        coerce_to_float64=False,
    )


@pytest.mark.parametrize(
    "aggfunc",
    [
        "count",
        "sum",
    ],
)
@pytest.mark.parametrize(
    "values",
    [
        "DS",
        "FV",
    ],
)
@sql_count_checker(query_count=1)
def test_pivot_table_with_sum_and_count_null_and_empty_values_matching_behavior(
    df_data_small, aggfunc, values
):
    pivot_table_test_helper(
        df_data_small,
        {"index": ["ax", "AX"], "columns": "aX", "values": values, "aggfunc": aggfunc},
    )


@pytest.mark.skip(
    "SNOW-870145: This fails because nan values are not stored as null so we count/sum them differently"
)
@pytest.mark.parametrize(
    "aggfunc",
    [
        "count",
        "sum",
    ],
)
def test_pivot_table_with_sum_and_count_null_and_empty_values_matching_behavior_skipped(
    df_data_small, aggfunc, values
):
    pivot_table_test_helper(
        df_data_small,
        {"index": ["AX", "aX"], "columns": "ax", "values": "ET", "aggfunc": aggfunc},
    )


@sql_count_checker(query_count=5, join_count=1)
def test_pivot_on_inline_data_using_temp_table():
    # Create a large dataframe of inlined data that will spill to a temporary table.
    snow_df = pd.DataFrame(
        {k: list(range(25)) for k in list("ABCDEFGHIJKLMNOPQRSTUVWXYZ")},
        index=pd.Index(list(range(25)), name="index_no"),
    )

    snow_df = snow_df.pivot_table(
        index="index_no", values="A", columns="B", aggfunc=["sum", "count"]
    )

    # This would fail if the inlined data was not materialized first.
    row_count = snow_df._query_compiler.get_axis_len(0)

    assert row_count == 25


@pytest.mark.xfail(strict=True, raises=SnowparkSQLException, reason="SNOW-1233895")
def test_pivot_empty_frame_snow_1233895():
    eval_snowpark_pandas_result(
        *create_test_dfs(columns=["a", "b", "c"]),
        lambda df: df.pivot_table(index="a", columns="b")
    )
