#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

# Tests for DataFrame.attrs, which allows users to locally store some metadata.
# This metadata is preserved across most DataFrame/Series operators.

import modin.pandas as pd
import pandas as native_pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.utils import create_test_dfs, eval_snowpark_pandas_result
from tests.integ.utils.sql_counter import sql_count_checker


def attrs_comparator(snow, native):
    return snow.attrs == native.attrs


@sql_count_checker(query_count=0)
def test_df_attrs_set_deepcopy():
    # When attrs is set to a new value, a deep copy is made:
    # >>> df = native_pd.DataFrame()
    # >>> d = {"a": 1}
    # >>> df.attrs = d
    # >>> df.attrs
    # {'a' : 1}
    # >>> d["a"] = 2
    # >>> df.attrs
    # {'a' : 1}
    def func(df):
        d = {"a": 1}
        df.attrs = d
        d["a"] = 2
        return df

    eval_snowpark_pandas_result(
        *create_test_dfs([]),
        func,
        comparator=attrs_comparator,
    )


@sql_count_checker(query_count=0)
def test_df_attrs_get_no_copy():
    # When df.attrs is read, the value can be modified:
    # >>> df = native_pd.DataFrame()
    # >>> df.attrs
    # {}
    # >>> d = df.attrs
    # >>> d["k"] = 1
    # >>> d
    # {'k': 1}
    # >>> df.attrs
    # {'k': 1}
    def func(df):
        d = df.attrs
        d["k"] = 1
        return df

    eval_snowpark_pandas_result(
        *create_test_dfs([]),
        func,
        comparator=attrs_comparator,
    )


# Tests that attrs is preserved across `take`, a unary operation that returns a Snowpark pandas object.
# Other unary operators are checked by other tests in the `eval_snowpark_pandas_result` method.
@sql_count_checker(query_count=0)
def test_df_attrs_take():
    def func(df):
        df.attrs = {"A": [1], "B": "check me"}
        return df.take([1])

    eval_snowpark_pandas_result(
        *create_test_dfs([1, 2]),
        func,
        comparator=attrs_comparator,
    )


# Tests that attrs only copies the attrs of its first argument.
# Other binary operators are checked by other tests in the `eval_snowpark_pandas_result` method.
@sql_count_checker(query_count=0)
def test_df_attrs_add():
    def func(df):
        df.attrs = {"A": [1], "B": "check me"}
        if isinstance(df, pd.DataFrame):
            other = pd.DataFrame([2, 3])
            other.attrs = {"C": "bad attrs"}
            return df + other
        other = native_pd.DataFrame([2, 3])
        other.attrs = {"C": "bad attrs"}
        return df + other

    eval_snowpark_pandas_result(
        *create_test_dfs([1, 2]), func, comparator=attrs_comparator
    )


# Tests that attrs is copied through `pd.concat` only when the attrs of all input frames match.
@pytest.mark.parametrize(
    "data, attrs_list",
    [
        (
            [[1], [2], [3]],
            [{"a": 1}, {"b": 2}, {"b": 2}],
        ),  # mismatched attrs, don't propagate
        (
            [[1], [2], [3]],
            [{"a": 1}, {"a": 2}, {"a": 2}],
        ),  # mismatched attrs, don't propagate
        ([[1], [2], [3]], [{"a": 1}, {"a": 1}, {"a": 1}]),  # same attrs, do propagate
    ],
)
@pytest.mark.parametrize("axis", [0, 1])
@sql_count_checker(query_count=0)
def test_df_attrs_concat(data, attrs_list, axis):
    native_dfs = [native_pd.DataFrame(arr) for arr in data]
    snow_dfs = [pd.DataFrame(arr) for arr in data]
    # attrs is not copied through the DataFrame constructor, so we need to copy it manually
    for snow_df, native_df, attrs in zip(snow_dfs, native_dfs, attrs_list):
        native_df.attrs = attrs
        snow_df.attrs = attrs
    native_result = native_pd.concat(native_dfs, axis=axis)
    snow_result = pd.concat(snow_dfs, axis=axis)
    attrs_comparator(snow_result, native_result)
