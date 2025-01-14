#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import re

import modin.pandas as pd
import pandas as native_pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.utils import (
    assert_snowpark_pandas_equals_to_pandas_without_dtypecheck,
    create_test_dfs,
    eval_snowpark_pandas_result,
)
from tests.integ.utils.sql_counter import SqlCounter, sql_count_checker


@sql_count_checker(query_count=1, join_count=2)
def test_assign_basic_series():
    snow_df, native_df = create_test_dfs(
        [[1, 2, 3], [4, 5, 6], [7, 8, 9]],
        columns=native_pd.Index(list("abc"), name="columns"),
        index=native_pd.Index([0, 1, 2], name="index"),
    )
    native_df.columns.names = ["columns"]
    native_df.index.names = ["index"]

    def assign_func(df):
        if isinstance(df, pd.DataFrame):
            return df.assign(new_col=pd.Series([10, 11, 12]))
        else:
            return df.assign(new_col=native_pd.Series([10, 11, 12]))

    eval_snowpark_pandas_result(snow_df, native_df, assign_func)


@sql_count_checker(query_count=1, join_count=2)
@pytest.mark.parametrize(
    "index", [[2, 1, 0], [4, 5, 6]], ids=["reversed_index", "different_index"]
)
def test_assign_basic_series_mismatched_index(index):
    snow_df, native_df = create_test_dfs(
        [[1, 2, 3], [4, 5, 6], [7, 8, 9]],
        columns=native_pd.Index(list("abc"), name="columns"),
        index=native_pd.Index([0, 1, 2], name="index"),
    )
    native_df.columns.names = ["columns"]
    native_df.index.names = ["index"]

    def assign_func(df):
        if isinstance(df, pd.DataFrame):
            return df.assign(new_col=pd.Series([10, 11, 12], index=index))
        else:
            return df.assign(new_col=native_pd.Series([10, 11, 12], index=index))

    eval_snowpark_pandas_result(snow_df, native_df, assign_func)


@pytest.mark.parametrize("new_col_value", [2, [10, 11, 12], "x"])
def test_assign_basic_non_pandas_object(new_col_value):
    join_count = 3 if isinstance(new_col_value, list) else 1
    with SqlCounter(query_count=1, join_count=join_count):
        snow_df, native_df = create_test_dfs(
            [[1, 2, 3], [4, 5, 6], [7, 8, 9]],
            columns=native_pd.Index(list("abc"), name="columns"),
            index=native_pd.Index([0, 1, 2], name="index"),
        )
        native_df.columns.names = ["columns"]
        native_df.index.names = ["index"]
        eval_snowpark_pandas_result(
            snow_df, native_df, lambda df: df.assign(new_column=new_col_value)
        )


@sql_count_checker(query_count=1, join_count=3)
def test_assign_invalid_long_column_length_negative():
    # pandas errors out in this test, since we are attempting to assign a column of length 5 to a DataFrame with length 3.
    # Snowpark pandas on the other hand, just truncates the last element of the new column so that it is the correct length. If we wanted
    # to error and match pandas behavior, we'd need to eagerly materialize the DataFrame to confirm lengths are correct
    # and error otherwise.
    snow_df, native_df = create_test_dfs(
        [[1, 2, 3], [4, 5, 6], [7, 8, 9]],
        columns=native_pd.Index(list("abc"), name="columns"),
        index=native_pd.Index([0, 1, 2], name="index"),
    )
    native_df.columns.names = ["columns"]
    native_df.index.names = ["index"]
    with pytest.raises(
        ValueError,
        match=re.escape("Length of values (5) does not match length of index (3)"),
    ):
        native_df = native_df.assign(new_column=[10, 11, 12, 13, 14])

    snow_df = snow_df.assign(new_column=[10, 11, 12, 13, 14])
    native_df = native_df.assign(new_column=[10, 11, 12])
    assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(snow_df, native_df)


@sql_count_checker(query_count=1, join_count=3)
def test_assign_invalid_short_column_length_negative():
    # pandas errors out in this test, since we are attempting to assign a column of length 2 to a DataFrame with length 3.
    # Snowpark pandas on the other hand, just broadcasts the last element of the new column so that it is filled. If we wanted
    # to error and match pandas behavior, we'd need to eagerly materialize the DataFrame to confirm lengths are correct
    # and error otherwise.
    snow_df, native_df = create_test_dfs(
        [[1, 2, 3], [4, 5, 6], [7, 8, 9]],
        columns=native_pd.Index(list("abc"), name="columns"),
        index=native_pd.Index([0, 1, 2], name="index"),
    )
    native_df.columns.names = ["columns"]
    native_df.index.names = ["index"]
    with pytest.raises(
        ValueError,
        match=re.escape("Length of values (2) does not match length of index (3)"),
    ):
        native_df = native_df.assign(new_column=[10, 11])

    snow_df = snow_df.assign(new_column=[10, 11])
    native_df = native_df.assign(new_column=[10, 11, 11])
    assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(snow_df, native_df)


@sql_count_checker(query_count=1, join_count=2)
def test_assign_short_series():
    snow_df, native_df = create_test_dfs(
        [[1, 2, 3], [4, 5, 6], [7, 8, 9]],
        columns=native_pd.Index(list("abc"), name="columns"),
        index=native_pd.Index([0, 1, 2], name="index"),
    )
    native_df.columns.names = ["columns"]
    native_df.index.names = ["index"]
    snow_df = snow_df.assign(new_column=pd.Series([10, 11]))
    native_df = native_df.assign(new_column=native_pd.Series([10, 11]))
    assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(snow_df, native_df)


@sql_count_checker(query_count=1, join_count=2)
@pytest.mark.parametrize(
    "index", [[1, 0], [4, 5]], ids=["reversed_index", "different_index"]
)
def test_assign_short_series_mismatched_index(index):
    snow_df, native_df = create_test_dfs(
        [[1, 2, 3], [4, 5, 6], [7, 8, 9]],
        columns=native_pd.Index(list("abc"), name="columns"),
        index=native_pd.Index([0, 1, 2], name="index"),
    )
    native_df.columns.names = ["columns"]
    native_df.index.names = ["index"]
    snow_df = snow_df.assign(new_column=pd.Series([10, 11], index=index))
    native_df = native_df.assign(new_column=native_pd.Series([10, 11], index=index))
    assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(snow_df, native_df)


@sql_count_checker(query_count=1, join_count=1)
@pytest.mark.parametrize(
    "callable_fn",
    [lambda x: x["a"], lambda x: x["a"] + x["b"]],
    ids=["identity_fn", "add_two_cols_fn"],
)
def test_assign_basic_callable(callable_fn):
    snow_df, native_df = create_test_dfs(
        [[1, 2, 3], [4, 5, 6], [7, 8, 9]],
        columns=native_pd.Index(list("abc"), name="columns"),
        index=native_pd.Index([0, 1, 2], name="index"),
    )
    native_df.columns.names = ["columns"]
    native_df.index.names = ["index"]
    eval_snowpark_pandas_result(
        snow_df, native_df, lambda df: df.assign(new_col=callable_fn)
    )


@sql_count_checker(query_count=1, join_count=1)
def test_assign_chained_callable():
    snow_df, native_df = create_test_dfs(
        [[1, 2, 3], [4, 5, 6], [7, 8, 9]],
        columns=native_pd.Index(list("abc"), name="columns"),
        index=native_pd.Index([0, 1, 2], name="index"),
    )
    native_df.columns.names = ["columns"]
    native_df.index.names = ["index"]
    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        lambda df: df.assign(
            new_col=lambda x: x["a"] + x["b"], last_col=lambda x: x["new_col"] ** 2
        ),
    )


@sql_count_checker(query_count=0)
def test_assign_chained_callable_wrong_order():
    snow_df, native_df = create_test_dfs(
        [[1, 2, 3], [4, 5, 6], [7, 8, 9]],
        columns=native_pd.Index(list("abc"), name="columns"),
        index=native_pd.Index([0, 1, 2], name="index"),
    )
    native_df.columns.names = ["columns"]
    native_df.index.names = ["index"]
    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        lambda df: df.assign(
            last_col=lambda x: x["new_col"] ** 2, new_col=lambda x: x["a"] + x["b"]
        ),
        expect_exception=True,
        assert_exception_equal=True,
        expect_exception_match="new_col",
        expect_exception_type=KeyError,
    )


@sql_count_checker(query_count=1, join_count=1)
def test_assign_self_columns():
    snow_df, native_df = create_test_dfs(
        [[1, 2, 3], [4, 5, 6], [7, 8, 9]],
        columns=native_pd.Index(list("abc"), name="columns"),
        index=native_pd.Index([0, 1, 2], name="index"),
    )
    native_df.columns.names = ["columns"]
    native_df.index.names = ["index"]
    eval_snowpark_pandas_result(
        snow_df, native_df, lambda df: df.assign(new_col=df["a"], last_col=df["b"])
    )


@sql_count_checker(query_count=1, join_count=3)
def test_overwrite_columns_via_assign():
    snow_df, native_df = create_test_dfs(
        [[1, 2, 3], [4, 5, 6], [7, 8, 9]],
        columns=native_pd.Index(list("abc"), name="columns"),
        index=native_pd.Index([0, 1, 2], name="index"),
    )
    native_df.columns.names = ["columns"]
    native_df.index.names = ["index"]
    eval_snowpark_pandas_result(
        snow_df, native_df, lambda df: df.assign(a=df["b"], last_col=[10, 11, 12])
    )


@sql_count_checker(query_count=1, join_count=2)
def test_assign_basic_timedelta_series():
    snow_df, native_df = create_test_dfs(
        [[1, 2, 3], [4, 5, 6], [7, 8, 9]],
        columns=native_pd.Index(list("abc"), name="columns"),
        index=native_pd.Index([0, 1, 2], name="index"),
    )
    native_df.columns.names = ["columns"]
    native_df.index.names = ["index"]

    native_td = native_pd.timedelta_range("1 day", periods=3)

    def assign_func(df):
        if isinstance(df, pd.DataFrame):
            return df.assign(new_col=pd.Series(native_td))
        else:
            return df.assign(new_col=native_pd.Series(native_td))

    eval_snowpark_pandas_result(snow_df, native_df, assign_func)
