#!/usr/bin/env python3
#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import numpy as np
import pandas as native_pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.pivot.pivot_utils import pivot_table_test_helper
from tests.integ.modin.utils import eval_snowpark_pandas_result
from tests.integ.utils.sql_counter import SqlCounter, sql_count_checker


@sql_count_checker(query_count=1, join_count=1)
@pytest.mark.modin_sp_precommit
def test_pivot_table_single_index_single_column_multiple_values(df_data):
    pivot_table_test_helper(
        df_data,
        {
            "index": "A",
            "columns": "B",
            "values": ["D", "E"],
        },
    )


@sql_count_checker(query_count=1, union_count=1)
def test_pivot_table_no_index_single_column_multiple_values(df_data):
    pivot_table_test_helper(
        df_data,
        {
            "columns": "B",
            "values": ["D", "E"],
        },
    )


@sql_count_checker(query_count=1, union_count=1, join_count=2)
def test_pivot_table_no_index_single_column_multiple_values_multiple_aggr_func(df_data):
    pivot_table_test_helper(
        df_data,
        {
            "columns": "B",
            "values": ["D", "E"],
            "aggfunc": ["mean", "max"],
        },
    )


@sql_count_checker(query_count=1, union_count=1)
@pytest.mark.parametrize("columns", ["B", ["B", "C"]])
def test_pivot_table_no_index_multiple_values_single_aggr_func_dict(df_data, columns):
    pivot_table_test_helper(
        df_data,
        {
            "columns": columns,
            "values": ["D", "E"],
            "aggfunc": {"D": "mean", "E": "max"},
        },
    )


# pandas moves the name of the aggfunc into the data columns as an index column.
@pytest.mark.xfail(
    strict=True,
    reason="SNOW-1435365 - look into no index + aggfunc as dictionary with list.",
)
@sql_count_checker(query_count=1, union_count=1)
@pytest.mark.parametrize("columns", ["B", ["B", "C"]])
def test_pivot_table_no_index_column_multiple_values_multiple_aggr_func_dict(
    df_data, columns
):
    pivot_table_test_helper(
        df_data,
        {
            "columns": columns,
            "values": ["D", "E"],
            "aggfunc": {"D": ["mean", "sum"], "E": "max"},
        },
    )


@sql_count_checker(query_count=1, join_count=1)
def test_pivot_table_no_index_single_column_single_values_multiple_aggr_func(df_data):
    pivot_table_test_helper(
        df_data,
        {
            "columns": "B",
            "values": "D",
            "aggfunc": ["mean", "max"],
        },
    )


@pytest.mark.parametrize("aggfunc", ["count", "sum", "min", "max", "mean", ["count"]])
@pytest.mark.parametrize("values", ["D", ["D"]])
@pytest.mark.parametrize(
    "named_columns", [pytest.param(True, marks=pytest.mark.xfail()), False]
)
@sql_count_checker(query_count=1)
def test_pivot_table_single_index_multiple_column_single_value(
    df_data,
    aggfunc,
    values,
    named_columns,
):
    pivot_table_test_helper(
        df_data,
        {
            "index": "A",
            "columns": ["B", "C"],
            "values": values,
            "aggfunc": aggfunc,
        },
        named_columns=named_columns,
    )


@pytest.mark.parametrize("aggfunc", ["count", "sum", "min", "max", "mean"])
@pytest.mark.parametrize("values", ["D", ["D"]])
@sql_count_checker(query_count=1)
def test_pivot_table_no_index_multiple_column_single_value(df_data, aggfunc, values):
    pivot_table_test_helper(
        df_data,
        {
            "columns": ["B", "C"],
            "values": values,
            "aggfunc": aggfunc,
        },
    )


@pytest.mark.parametrize("values", ["D", ["D"]])
@sql_count_checker(query_count=1, join_count=1)
def test_pivot_table_no_index_multiple_column_single_value_multiple_aggr_func(
    df_data, values
):
    pivot_table_test_helper(
        df_data,
        {
            "columns": ["B", "C"],
            "values": values,
            "aggfunc": ["mean", "max"],
        },
    )


@pytest.mark.skip(
    "SNOW-853416: Some lingering encoding issues and also unsorted order does not match"
)
def test_pivot_table_single_index_single_column_multiple_encoded_values(
    df_encoded_data,
):
    pivot_table_test_helper(
        df_encoded_data,
        {
            "index": "A",
            "columns": ["B", "C"],
            "values": "D",
        },
    )


@sql_count_checker(query_count=1)
def test_pivot_table_single_index_single_column_multiple_small_encoded_values(
    df_small_encoded_data,
):
    pivot_table_test_helper(
        df_small_encoded_data,
        {
            "index": "A",
            "columns": ["B", "C"],
            "values": "D",
        },
    )


@pytest.mark.skip(
    "SNOW-853416: Some lingering encoding issues and also unsorted order does not match"
)
def test_pivot_table_single_index_single_column_multiple_encoded_values_with_sort(
    df_encoded_data,
):
    pivot_table_test_helper(
        df_encoded_data,
        {
            "index": "A",
            "columns": ["B", "C"],
            "values": "D",
            "sort": True,
        },
    )


@pytest.mark.parametrize("aggfunc", ["count", ["count"]])
@sql_count_checker(query_count=1, join_count=1)
def test_pivot_table_single_index_multiple_columns_multiple_values(df_data, aggfunc):
    pivot_table_test_helper(
        df_data,
        {
            "index": "A",
            "columns": ["B", "C"],
            "values": ["D", "E"],
            "aggfunc": aggfunc,
        },
    )


@pytest.mark.parametrize("aggfunc", ["count", ["count"]])
@sql_count_checker(query_count=1, union_count=1)
def test_pivot_table_no_index_multiple_columns_multiple_values(df_data, aggfunc):
    pivot_table_test_helper(
        df_data,
        {
            "columns": ["B", "C"],
            "values": ["D", "E"],
            "aggfunc": aggfunc,
        },
    )


@sql_count_checker(query_count=1, union_count=1, join_count=2)
def test_pivot_table_no_index_multiple_columns_multiple_values_multiple_aggr_funcs(
    df_data,
):
    pivot_table_test_helper(
        df_data,
        {
            "columns": ["B", "C"],
            "values": ["D", "E"],
            "aggfunc": ["mean", "max"],
        },
    )


@sql_count_checker(query_count=1, join_count=1)
def test_pivot_table_multiple_index_multiple_columns_multiple_values(df_data):
    pivot_table_test_helper(
        df_data,
        {
            "index": ["A", "F"],
            "columns": ["B", "C"],
            "values": ["D", "E"],
        },
    )


@sql_count_checker(query_count=1, join_count=9)
def test_pivot_table_single_index_no_column_single_value_multiple_aggr_funcs(df_data):
    pivot_table_test_helper(
        df_data,
        {
            "index": "A",
            "columns": None,
            "values": None,
            "aggfunc": ["min", "max"],
        },
    )


@sql_count_checker(query_count=0)
def test_pivot_table_no_index_no_column_single_value(df_data):
    pivot_kwargs = {
        "values": "D",
        "aggfunc": "mean",
    }
    eval_snowpark_pandas_result(
        pd.DataFrame(df_data),
        native_pd.DataFrame(df_data),
        lambda df: df.pivot_table(**pivot_kwargs),
        assert_exception_equal=True,
        expect_exception=True,
        expect_exception_match="No group keys passed!",
        expect_exception_type=ValueError,
    )


@sql_count_checker(query_count=0)
def test_pivot_table_no_index_no_column_single_value_multiple_aggr_funcs(df_data):
    pivot_kwargs = {
        "values": "D",
        "aggfunc": ["mean", "max"],
    }
    eval_snowpark_pandas_result(
        pd.DataFrame(df_data),
        native_pd.DataFrame(df_data),
        lambda df: df.pivot_table(**pivot_kwargs),
        assert_exception_equal=True,
        expect_exception=True,
        expect_exception_match="No group keys passed!",
        expect_exception_type=ValueError,
    )


@sql_count_checker(query_count=0, join_count=0)
def test_pivot_table_no_index_no_column_no_value_multiple_aggr_funcs(df_data):
    pivot_kwargs = {
        "columns": None,
        "values": None,
        "aggfunc": ["min", "max"],
    }
    eval_snowpark_pandas_result(
        pd.DataFrame(df_data),
        native_pd.DataFrame(df_data),
        lambda df: df.pivot_table(**pivot_kwargs),
        assert_exception_equal=True,
        expect_exception=True,
        expect_exception_match="No group keys passed!",
        expect_exception_type=ValueError,
    )


@pytest.mark.skip(
    "SNOW-854301: Multi-Index replaces None with Nan causing test to fail"
)
@pytest.mark.parametrize(
    "values",
    [
        [None],
        ["D", None],
    ],
)
def test_pivot_table_multiple_index_multiple_columns_null_values(df_data, values):
    def update_columns_inline(df):
        df.columns = ["A", "B", "C", "D", None, "F"]
        return df

    pivot_table_test_helper(
        df_data,
        {
            "index": ["A"],
            "columns": ["B", "C"],
            "values": values,
        },
        preprocess_df=lambda df: update_columns_inline(df),
    )


# TODO (SNOW-854301): Needs support for MultiIndex.levels, fails because result.columns.levels[N] don't equal
# We use xfail to run so we can help code coverage
@pytest.mark.xfail(strict=True)
@pytest.mark.parametrize("values", [None, []])
@sql_count_checker(query_count=0)
def test_pivot_table_no_values_by_default(df_data, values):
    pivot_table_test_helper(
        df_data,
        {
            "index": ["A", "B", "C"],
            "columns": ["D", "E", "F"],
            "values": values,
            "aggfunc": "min",
        },
    )


@sql_count_checker(query_count=1, join_count=1)
def test_pivot_table_single_index_single_column_single_value_multiple_aggr_funcs(
    df_data,
):
    pivot_table_test_helper(
        df_data,
        {"index": "A", "columns": "C", "values": "D", "aggfunc": ["min", "max"]},
    )


@sql_count_checker(query_count=1, join_count=1)
def test_pivot_table_single_index_single_column_single_value_multiple_duplicate_aggr_funcs(
    df_data,
):
    pivot_table_test_helper(
        df_data,
        {"index": "A", "columns": "C", "values": "D", "aggfunc": ["min", "min"]},
    )


@sql_count_checker(query_count=1, join_count=3)
def test_pivot_table_single_index_multiple_column_multiple_values_multiple_duplicate_aggr_funcs(
    df_data,
):
    pivot_table_test_helper(
        df_data,
        {
            "index": "A",
            "columns": ["B", "C"],
            "values": ["D", "E"],
            "aggfunc": ["sum", "sum"],
        },
    )


@pytest.mark.parametrize(
    "aggfunc, expected_join_count",
    [
        ("mean", 0),
        (["min", "max"], 1),
        ({"D": "count"}, 0),
        ({"D": ["sum"]}, 0),
        ({"D": ["min", "max"]}, 1),
    ],
)
def test_pivot_table_multiple_index_single_column_single_value_multiple_aggr_funcs(
    df_data,
    expected_join_count,
    aggfunc,
):
    with SqlCounter(query_count=1, join_count=expected_join_count):
        pivot_table_test_helper(
            df_data,
            {
                "index": ["A", "B"],
                "columns": "C",
                "values": "D",
                "aggfunc": aggfunc,
            },
        )


@sql_count_checker(query_count=1, join_count=1)
def test_pivot_table_multiple_index_multiple_column_single_value_multiple_aggr_funcs(
    df_data,
):
    pivot_table_test_helper(
        df_data,
        {
            "index": ["A", "F"],
            "columns": ["B", "C"],
            "values": "D",
            "aggfunc": ["min", "max"],
        },
    )


@pytest.mark.parametrize(
    "aggfunc, expected_join_count",
    [
        ({"D": [np.min, "count"], "E": [np.mean, np.sum]}, 3),
        ([np.min, np.max], 3),
        # Aggregation column E should be dropped since it is not mentioned here.
        ({"D": ["min", "max"]}, 1),
    ],
)
def test_pivot_table_multiple_index_multiple_column_multiple_values_multiple_aggr_funcs(
    df_data,
    aggfunc,
    expected_join_count,
):
    with SqlCounter(query_count=1, join_count=expected_join_count):
        pivot_table_test_helper(
            df_data,
            {
                "index": ["A", "F"],
                "columns": ["B", "C"],
                "values": ["D", "E"],
                "aggfunc": aggfunc,
            },
        )


@pytest.mark.parametrize("index_keys", ["A", ["A", "B"], "B"])
@sql_count_checker(query_count=1)
def test_pivot_table_with_initial_index_and_multiple_columns(df_data, index_keys):
    pivot_table_test_helper(
        df_data,
        pivot_table_kwargs={
            "index": "A",
            "columns": ["B", "C"],
            "values": "D",
        },
        preprocess_df=lambda df: df.set_index(keys=index_keys),
    )
