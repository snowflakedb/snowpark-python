#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import itertools
import random
from itertools import chain, combinations

import modin.pandas as pd
import numpy as np
import pandas as native_pd
import pytest
from pandas import isna

import snowflake.snowpark.modin.plugin  # noqa: F401
from snowflake.snowpark.modin.plugin.extensions.utils import try_convert_index_to_native
from tests.integ.modin.utils import (
    assert_frame_equal,
    assert_snowpark_pandas_equals_to_pandas_without_dtypecheck,
    create_test_dfs,
    eval_snowpark_pandas_result,
)
from tests.integ.utils.sql_counter import SqlCounter, sql_count_checker


@pytest.mark.parametrize(
    "key",
    [
        [True, True, False, False, False, True, True],
        [True] * 7,
        [False] * 7,
        np.array([True, True, False, False, False, True, True], dtype=bool),
        native_pd.Index([True, True, False, False, False, True, True]),
        [True],
        [True, True, False, False, False, True, True, True],
        native_pd.Index([], dtype=bool),
        np.array([], dtype=bool),
    ],
)
@sql_count_checker(query_count=1, join_count=1)
def test_df_getitem_with_boolean_list_like(
    key, default_index_snowpark_pandas_df, default_index_native_df
):
    # df[boolean list-like key] is the same as df.loc[:, boolean list-like key]

    def get_helper(df, key):
        if isinstance(df, pd.DataFrame):
            if isinstance(key, native_pd.Index):
                key = pd.Index(key)
            return df[key]
        # If pandas df, adjust the length of the df and key since boolean keys need to be the same length as the axis.
        _key = try_convert_index_to_native(key)
        _df = df.iloc[: len(key)]
        _key = _key[: _df.shape[1]]
        return _df[_key]

    eval_snowpark_pandas_result(
        default_index_snowpark_pandas_df,
        default_index_native_df,
        lambda df: get_helper(df, key),
    )


@pytest.mark.parametrize(
    "key",
    [
        [random.choice("ABCDEFG") for _ in range(random.randint(1, 20))],
        native_pd.Index(random.choice("ABCDEFG") for _ in range(random.randint(1, 20))),
        np.array([random.choice("ABCDEFG") for _ in range(random.randint(1, 20))]),
    ],
)
def test_df_getitem_with_string_list_like(
    key, default_index_snowpark_pandas_df, default_index_native_df
):
    # df[string list-like key] is the same as df.loc[:, string list-like key]
    if isinstance(key, native_pd.Index):
        snow_key = pd.Index(key)
    else:
        snow_key = key

    def get_helper(df):
        if isinstance(df, pd.DataFrame):
            return df[snow_key]
        else:
            return df[key]

    # 4 extra queries for iter
    with SqlCounter(query_count=5 if isinstance(key, native_pd.Index) else 1):
        eval_snowpark_pandas_result(
            default_index_snowpark_pandas_df,
            default_index_native_df,
            get_helper,
        )


@pytest.mark.parametrize(
    "key",
    [
        [random.choice(range(7)) for _ in range(random.randint(1, 20))],
        native_pd.Index(random.choice(range(7)) for _ in range(random.randint(1, 20))),
        np.array([random.choice(range(7)) for _ in range(random.randint(1, 20))]),
    ],
)
def test_df_getitem_with_int_list_like(key):
    # df[int list-like key] is the same as df.loc[:, int list-like key]
    if isinstance(key, native_pd.Index):
        snow_key = pd.Index(key)
    else:
        snow_key = key

    def get_helper(df):
        if isinstance(df, pd.DataFrame):
            return df[snow_key]
        else:
            return df[key]

    # Generate a dict that maps from int -> list of random ints
    data = {i: [random.choice(range(10)) for _ in range(5)] for i in range(7)}
    native_df = native_pd.DataFrame(data)
    snowpark_df = pd.DataFrame(native_df)

    # 4 extra queries for iter
    with SqlCounter(query_count=5 if isinstance(key, native_pd.Index) else 1):
        eval_snowpark_pandas_result(
            snowpark_df,
            native_df,
            get_helper,
        )


def _powerset(iterable):
    s = list(iterable)
    combos = chain.from_iterable(combinations(s, r) for r in range(len(s) + 1))

    return [list(el) for el in combos]


@pytest.mark.parametrize("key", _powerset(["A", "B", "C"]))
@sql_count_checker(query_count=1)
def test_df_getitem_with_string_labels(key):
    data = {"A": [1, 2, None], "B": [3.1, 5, 6], "C": [None, "abc", "xyz"]}
    snow_df = pd.DataFrame(data)
    native_df = native_pd.DataFrame(data)
    eval_snowpark_pandas_result(snow_df, native_df, lambda df: df[key])


@pytest.mark.parametrize(
    "key", list(filter(lambda key: len(key) > 0, _powerset(["X", "Y", "Z"])))
)
@sql_count_checker(query_count=0)
def test_df_getitem_with_string_labels_throws_keyerror(key):
    data = {"A": [1, 2, None], "B": [3.1, 5, 6], "C": [None, "abc", "xyz"]}
    with pytest.raises(
        KeyError,
        match=r"None of .* are in the \[columns\]",
    ):
        eval_snowpark_pandas_result(
            *create_test_dfs(data),
            lambda df: df[key],
        )


LABEL_COLLECTION = ["A", None, 12, 4.56, ("a", "b"), (1, 2), np.nan]
N = 5
# generate dataframe based on LABEL collection
DATA = np.array([np.random.randint(0, 100, N) for name in LABEL_COLLECTION]).T


@pytest.mark.parametrize("key", LABEL_COLLECTION)
@sql_count_checker(query_count=1)
def test_df_getitem_with_labels_single_column(key):
    snow_df = pd.DataFrame(DATA, columns=LABEL_COLLECTION)
    native_df = native_pd.DataFrame(DATA, columns=LABEL_COLLECTION)
    eval_snowpark_pandas_result(snow_df, native_df, lambda df: df[key])


@sql_count_checker(query_count=0)
def test_df_getitem_with_none_nan_columns():
    # TODO SNOW-1201504: in pandas 2.1.4, indexing with pd.Index([None, nan], dtype=object) raises
    # ValueError: Cannot mask with non-boolean array containing NA / NaN values
    # This occurs because of a check in pandas index validation that removes nan values
    # from array keys:
    # https://github.com/pandas-dev/pandas/blob/6fa2a4be8fd4cc7c5746100d9883471a5a916661/pandas/core/common.py#L134-L139
    # This check was performed differently in pandas 2.0.3, and this indexing operation would retrieve
    # the columns corresponding to the (distinct) None and nan labels, as expected.
    #
    # In general, Snowpark pandas should treat None and nan as equivalent values. However, constructing
    # an Index with dtype=object will keep them as separate values. If dtype=object is not explicitly specified
    # (either in the columns of the DF constructor or in the indexing key), then None is implicitly coerced
    # to nan.
    key = native_pd.Index([None, np.nan], dtype=object)
    snow_df = pd.DataFrame(DATA, columns=LABEL_COLLECTION)
    native_df = native_pd.DataFrame(DATA, columns=LABEL_COLLECTION)
    eval_snowpark_pandas_result(
        snow_df,
        native_df,
        lambda df: df[key],
        expect_exception=True,
        expect_exception_type=ValueError,
        expect_exception_match="Cannot mask with non-boolean array containing NA / NaN values",
    )


@pytest.mark.parametrize(
    "key",
    [
        native_pd.Index(list(t), dtype="object")
        for t in itertools.combinations(LABEL_COLLECTION, 2)
        # This combination is covered by test_df_getitem_with_none_nan_columns
        if list(t) != [None, np.nan]
    ],
)
@sql_count_checker(query_count=1)
def test_df_getitem_with_labels_two_columns_with_index(key):
    snow_df = pd.DataFrame(DATA, columns=LABEL_COLLECTION)
    native_df = native_pd.DataFrame(DATA, columns=LABEL_COLLECTION)

    def helper(df):
        ans_df = df[key]
        # because columns is returned as an index object, and the missing value mapping is not 100% pandas,
        # cast here to_pandas with columns' index object type to compare and explicitly convert
        # Na to None to allow comparison
        columns = native_pd.Index(
            [None if isna(val) else val for val in ans_df.columns.values], "object"
        )
        if isinstance(ans_df, (pd.Series, pd.DataFrame)):
            ans_df = ans_df.to_pandas()
        ans_df.columns = columns
        return ans_df

    snow_res = helper(snow_df)
    native_res = helper(native_df)
    # For the column index for [12, None] the index type differs for Snowpark pandas from
    # pandas. For Snowpark pandas the inferred_type attribute is mixed, whereas it is mixed-integer for pandas.
    # I.e., Snowpark pandas has Index([None, 12.0], dtype='object') whereas pandas has Index([None, 12], dtype='object')
    # to allow for this scenario, set check_column_type=False here.
    assert_frame_equal(
        snow_res,
        native_res,
        check_dtype=False,
        check_column_type=False,
        check_index_type=False,
    )


@pytest.mark.parametrize("key_data", [[], ["A"], ["F", "A", "B"], ["A", "G", "F", "A"]])
def test_df_getitem_with_series(key_data):
    columns = ["A", "B", "C", "D", "E", "F", "G"]
    # set up Snowpark data
    snow_df = pd.DataFrame(DATA, columns=columns)
    snow_series = pd.Series(key_data)
    # set up native pandas data
    native_df = native_pd.DataFrame(DATA, columns=columns)
    native_series = native_pd.Series(key_data)

    # 1 query for iterating through the series to see if it's boolean, 1 query for to_pandas.
    with SqlCounter(query_count=2):
        # compute the results and compare
        snow_ans = snow_df[snow_series]
        native_ans = native_df[native_series]
        assert_snowpark_pandas_equals_to_pandas_without_dtypecheck(
            snow_ans,
            native_ans,
        )


@pytest.mark.parametrize("key", [["a", "b", "c"], ["A", "a"], ["a", "a", "a"]])
@sql_count_checker(query_count=0)
def test_df_getitem_throws_key_error(key):
    columns = ["A", "B", "C"]
    data = np.random.normal(size=(3, 3))
    snow_df = pd.DataFrame(data, columns=columns)
    native_df = native_pd.DataFrame(data, columns=columns)

    if all(k not in columns for k in key):
        match_str = r"None of .* are in the \[columns\]"
    else:
        match_str = r".* not in index"

    with pytest.raises(
        KeyError,
        match=match_str,
    ):
        eval_snowpark_pandas_result(
            snow_df,
            native_df,
            lambda df: df[key],
        )


@sql_count_checker(query_count=2)
def test_df_getitem_calls_getitem():
    N = 7
    data = {i: [i] * N for i in range(N)}
    native_df = native_pd.DataFrame(data)
    snowpark_df = pd.DataFrame(native_df)
    eval_snowpark_pandas_result(
        snowpark_df,
        native_df,
        lambda df: df[df[1]],
    )


@pytest.mark.parametrize(
    "key",
    [
        slice(1, 4, 2),  # start < stop, step > 0
        slice(1, 4, -2),  # start < stop, step < 0
        slice(-1, -4, 2),  # start > stop, step > 0
        slice(-1, -4, -2),  # start > stop, step < 0
        slice(3, -1, 4),
        slice(5, 1, -36897),
        # start = step
        slice(3, -1, 4),  # step > 0
        slice(100, 100, 1245),  # step > 0
        slice(-100, -100, -3),  # step < 0
        slice(-100, -100, -36897),  # step < 0
        slice(2, 1, -2),
        # with None
        slice(None, 2, 1),
        slice(-100, None, -2),
    ],
)
@sql_count_checker(query_count=1)
def test_df_getitem_with_slice(
    key, default_index_snowpark_pandas_df, default_index_native_df
):
    eval_snowpark_pandas_result(
        default_index_snowpark_pandas_df,
        default_index_native_df,
        lambda df: df[key],
    )


@pytest.mark.parametrize(
    "key",
    [
        slice("x", "y"),
        slice("x", "z", 2),
        slice("a", "z", -1),
        slice("z", "a", -1),
    ],
)
@sql_count_checker(query_count=1, join_count=0)
def test_df_getitem_with_non_int_slice(key):
    data = {"a": [1, 2, 3], "b": [4, 5, 6]}
    index = ["x", "y", "z"]
    snow_df = pd.DataFrame(data, index=index)
    native_df = native_pd.DataFrame(data, index=index)

    eval_snowpark_pandas_result(snow_df, native_df, lambda df: df[key], inplace=True)


@pytest.mark.parametrize(
    "key",
    [
        np.array([True, True, False, False, False, True, True], dtype=bool),
        [],
        slice(2, 6, 4),
        "baz",
        [("foo", "one"), ("bar", "two")],
    ],
)
def test_df_getitem_with_multiindex(
    key, default_index_native_df, multiindex_native, native_df_with_multiindex_columns
):
    # Test __getitem__ with df with MultiIndex index.
    native_df = default_index_native_df.set_index(multiindex_native)
    snowpark_df = pd.DataFrame(native_df)
    # Keys below are column labels that don't exist in the current df - switch them to labels that exist.
    _key = "A" if isinstance(key, str) else key
    _key = (
        ["D", "G"]
        if (
            isinstance(key, list)
            and (sorted(key) == sorted([("foo", "one"), ("bar", "two")]))
        )
        else _key
    )
    with SqlCounter(query_count=1):
        eval_snowpark_pandas_result(snowpark_df, native_df, lambda df: df[_key])

    # Test __getitem__ with df with MultiIndex columns.
    native_df = native_df_with_multiindex_columns
    snowpark_df = pd.DataFrame(native_df)
    with SqlCounter(query_count=1):
        eval_snowpark_pandas_result(
            snowpark_df, native_df, lambda df: df[key], check_column_type=False
        )

    # Test __getitem__ with df with MultiIndex index.
    native_df = native_df_with_multiindex_columns.set_index(multiindex_native)
    snowpark_df = pd.DataFrame(native_df)
    with SqlCounter(query_count=1):
        eval_snowpark_pandas_result(
            snowpark_df, native_df, lambda df: df[key], check_column_type=False
        )


@sql_count_checker(query_count=1)
def test_df_getitem_lambda_dataframe():
    data = {"a": [1, 2, 3], "b": [4, 5, 6]}
    eval_snowpark_pandas_result(*create_test_dfs(data), lambda df: df[lambda x: x < 2])


@sql_count_checker(query_count=1)
def test_df_getitem_boolean_df_comparator():
    """
    DataFrame keys (as a result of callables) are valid for getitem but not loc and iloc get.
    """
    eval_snowpark_pandas_result(
        *create_test_dfs(
            [[1, 2, 3, 4], [5, 6, 7, 8], [9, 10, 11, 12], [13, 14, 15, 16]]
        ),
        lambda df: df[df > 7],
    )


@sql_count_checker(query_count=1)
def test_df_getitem_boolean_df_comparator_datetime_index():
    """
    Based on bug from SNOW-1348621.
    Code adapted from the pandas 10 minute quick start (https://pandas.pydata.org/docs/user_guide/10min.html).
    """
    dates = native_pd.date_range("20130101", periods=6)
    data = np.random.randn(6, 4)
    native_df = native_pd.DataFrame(data, index=dates, columns=list("ABCD"))
    snow_df = pd.DataFrame(native_df)
    eval_snowpark_pandas_result(
        snow_df, native_df, lambda df: df[df > 0], check_freq=False
    )
