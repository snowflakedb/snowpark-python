#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import re

import modin.pandas as pd
import numpy as np
import pandas as native_pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.utils import (
    assert_snowpark_pandas_equals_to_pandas_without_dtypecheck,
    create_test_dfs,
    eval_snowpark_pandas_result as _eval_snowpark_pandas_result,
)
from tests.integ.utils.sql_counter import sql_count_checker


def eval_snowpark_pandas_result(*args, **kwargs):
    # Some calls to the native pandas function propagate attrs while some do not, depending on the values of its arguments.
    return _eval_snowpark_pandas_result(*args, test_attrs=False, **kwargs)


@sql_count_checker(query_count=0)
def test_invalid_named_agg_errors(basic_df_data):
    eval_snowpark_pandas_result(
        *create_test_dfs(basic_df_data),
        lambda df: df.groupby("col1").agg(args=80, valid_agg=("col2", min)),
        expect_exception=True,
        expect_exception_match="Must provide 'func' or tuples of '\\(column, aggfunc\\).",
        assert_exception_equal=False,  # There is a typo in the pandas exception.
        expect_exception_type=TypeError,
    )


@sql_count_checker(query_count=0)
def test_invalid_func_with_named_agg_errors(basic_df_data):
    eval_snowpark_pandas_result(
        *create_test_dfs(basic_df_data),
        lambda df: df.groupby("col1").agg(80, valid_agg=("col2", min)),
        expect_exception=True,
        expect_exception_match="aggregation function is not callable",
        assert_exception_equal=False,
        # native pandas raises a TypeError instead
        expect_exception_type=ValueError,
    )


@sql_count_checker(query_count=0)
def test_valid_func_with_named_agg_errors(basic_df_data):
    eval_snowpark_pandas_result(
        *create_test_dfs(basic_df_data),
        lambda df: df.groupby("col1").agg(max, new_col=("col2", min)),
        expect_exception=True,
        assert_exception_equal=False,  # There is a difference in our errors.
        expect_exception_type=TypeError,
    )


@sql_count_checker(query_count=1)
def test_named_agg_output_column_order(basic_df_data):
    eval_snowpark_pandas_result(
        *create_test_dfs(basic_df_data),
        lambda df: df.groupby("col1").agg(
            new_col1=("col1", min), new_col2=("col2", min), new_col3=("col1", max)
        ),
    )


@sql_count_checker(query_count=1)
def test_named_agg_output_column_order_with_dup_columns(basic_df_data):
    native_df = native_pd.DataFrame(basic_df_data).rename(columns={"col3": "col1"})
    basic_snowpark_pandas_df = pd.DataFrame(basic_df_data).rename(
        columns={"col3": "col1"}
    )

    with pytest.raises(
        AttributeError, match=re.escape("'DataFrame' object has no attribute 'name'")
    ):
        native_df.groupby("col4").agg(
            new_col1=("col1", min), new_col2=("col2", min), new_col3=("col1", max)
        )

    result_df = native_pd.DataFrame(
        [
            [1.0, 8.0, 5.0, 1.0, 8.0],
            [2.0, 4.0, 4.0, 2.0, 4.0],
            [0.0, 1.1, 5.0, 0.0, 1.1],
            [0.0, 10.0, 7.0, 0.0, 10.0],
            [1.0, 12.0, 36.0, 1.0, 12.0],
            [2.0, 3.1, 4.0, 2.0, 3.1],
        ],
        columns=["new_col1", "new_col1", "new_col2", "new_col3", "new_col3"],
        index=native_pd.Index([3, 5, 6, 15, 16, 17], name="col4"),
    )
    snow_df = basic_snowpark_pandas_df.groupby("col4").agg(
        new_col1=("col1", min), new_col2=("col2", min), new_col3=("col1", max)
    )
    assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(snow_df, result_df)


@sql_count_checker(query_count=1)
def test_named_agg_passed_in_via_star_kwargs(basic_df_data):
    kwargs = {"new_col1": ("col1", min), "new_col2": pd.NamedAgg("col2", min)}
    eval_snowpark_pandas_result(
        *create_test_dfs(basic_df_data),
        lambda df: df.groupby("col1").agg(**kwargs),
    )


@sql_count_checker(query_count=0)
def test_named_agg_with_invalid_function_raises_not_implemented(
    basic_df_data,
):
    basic_snowpark_pandas_df = pd.DataFrame(basic_df_data)
    native_pandas = native_pd.DataFrame(basic_df_data)
    eval_snowpark_pandas_result(
        basic_snowpark_pandas_df,
        native_pandas,
        lambda df: df.groupby("col1").agg(
            c1=("col2", "min"), c2=("col2", "random_function")
        ),
        expect_exception=True,
        expect_exception_match="'SeriesGroupBy' object has no attribute",
    )


@sql_count_checker(query_count=1)
@pytest.mark.parametrize("size_func", ["size", len])
def test_named_agg_count_vs_size(size_func):
    data = [[1, 2, 3], [1, 5, np.nan], [7, np.nan, 9]]
    native_df = native_pd.DataFrame(
        data, columns=["a", "b", "c"], index=["owl", "toucan", "eagle"]
    )
    snow_df = pd.DataFrame(native_df)
    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        lambda df: df.groupby("a").agg(
            l=("b", size_func), j=("c", size_func), m=("c", "count"), n=("b", "count")
        ),
    )


@sql_count_checker(query_count=1)
@pytest.mark.parametrize("size_func", ["size", len])
def test_named_agg_size_on_series(size_func):
    native_series = native_pd.Series([1, 2, 3, 3], index=["a", "a", "b", "c"])
    snow_series = pd.Series(native_series)
    eval_snowpark_pandas_result(
        snow_series,
        native_series,
        lambda series: series.groupby(level=0).agg(new_col=size_func),
    )


@pytest.mark.parametrize("as_index", [True, False])
@pytest.mark.parametrize("sort", [True, False])
@sql_count_checker(query_count=1)
def test_named_groupby_agg_with_incorrect_func(as_index, sort) -> None:
    basic_snowpark_pandas_df = pd.DataFrame(
        data=8 * [range(3)], columns=["a", "b", "c"]
    )
    basic_snowpark_pandas_df = basic_snowpark_pandas_df.groupby(["a", "b"]).sum()
    native_pandas = basic_snowpark_pandas_df.to_pandas()
    eval_snowpark_pandas_result(
        basic_snowpark_pandas_df,
        native_pandas,
        lambda df: df.groupby(by="a", sort=sort, as_index=as_index).agg(
            NEW_B=("b", "sum"), ACTIVE_DAYS=("c", "COUNT")
        ),
        expect_exception=True,
    )
